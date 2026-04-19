import asyncio
import io
import json
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.scanner import DuplicateScanner

app = FastAPI(title="DupFinder", version="1.0.0")

# Mount static files
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# In-memory job store: job_id -> job state dict
jobs: Dict[str, dict] = {}

# Thread pool for background scans
executor = ThreadPoolExecutor(max_workers=4)

VALID_STAGES = {"size", "partial", "full"}


class ScanRequest(BaseModel):
    path: str
    stages: List[str] = ["size", "partial", "full"]


@app.get("/")
async def root():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(index))


@app.post("/scan")
async def start_scan(request: ScanRequest):
    # Validate path
    if not request.path or not request.path.strip():
        raise HTTPException(status_code=400, detail="path must not be empty")

    # Validate stages
    requested_stages = [s.lower() for s in request.stages]
    invalid = set(requested_stages) - VALID_STAGES
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid stages: {invalid}")
    if not requested_stages:
        raise HTTPException(status_code=400, detail="At least one stage must be selected")

    job_id = str(uuid.uuid4())

    jobs[job_id] = {
        "status": "running",
        "path": request.path,
        "stages": requested_stages,
        "progress": {
            "stage": "starting",
            "progress": 0,
            "found": 0,
            "message": "Initializing scan…",
            "files_scanned": 0,
        },
        "results": None,
        "error": None,
    }

    # Submit scan to thread pool
    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_scan, job_id, request.path, requested_stages)

    return {"job_id": job_id}


def _run_scan(job_id: str, path: str, stages: List[str]):
    """Runs in a background thread via ThreadPoolExecutor."""
    job = jobs.get(job_id)
    if job is None:
        return

    def progress_callback(stage: str, progress_pct: float, message: str, found_count: int):
        if job_id not in jobs:
            return
        jobs[job_id]["progress"] = {
            "stage": stage,
            "progress": round(progress_pct, 1),
            "found": found_count,
            "message": message,
            "files_scanned": 0,
        }

    try:
        scanner = DuplicateScanner(callback=progress_callback)
        results = scanner.scan(path, stages)
        results["scan_root"] = path
        jobs[job_id]["results"] = results
        jobs[job_id]["status"] = "done"
        jobs[job_id]["progress"] = {
            "stage": "done",
            "progress": 100,
            "found": results["group_count"],
            "message": (
                f"Scan complete — {results['group_count']} duplicate groups found"
            ),
            "files_scanned": results["total_files"],
        }
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)
        jobs[job_id]["progress"] = {
            "stage": "error",
            "progress": 0,
            "found": 0,
            "message": f"Error: {exc}",
            "files_scanned": 0,
        }


@app.get("/scan/{job_id}/progress")
async def scan_progress(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    async def event_generator():
        while True:
            job = jobs.get(job_id)
            if not job:
                break
            payload = dict(job["progress"])
            payload["status"] = job["status"]
            yield f"data: {json.dumps(payload)}\n\n"
            if job["status"] in ("done", "error"):
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/scan/{job_id}/results")
async def scan_results(job_id: str):
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] == "running":
        raise HTTPException(status_code=202, detail="Scan still in progress")
    if job["status"] == "error":
        raise HTTPException(status_code=500, detail=job.get("error", "Unknown error"))
    return job["results"]


@app.get("/browse")
async def browse(path: str = Query("/")):
    p = Path(path).resolve()
    if not p.exists() or not p.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")

    dirs = []
    try:
        for item in sorted(p.iterdir(), key=lambda x: x.name.lower()):
            if item.is_dir() and not item.name.startswith("."):
                try:
                    dirs.append({"name": item.name, "path": str(item)})
                except PermissionError:
                    pass
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")

    parent = str(p.parent) if p.parent != p else None
    return {"current": str(p), "parent": parent, "dirs": dirs}


IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif"}


@app.get("/thumbnail")
async def get_thumbnail(path: str = Query(...), size: int = Query(200)):
    from PIL import Image

    file_path = Path(path)
    if file_path.suffix.lower() not in IMAGE_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Not a supported image file")
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    try:
        img = Image.open(str(file_path))
        img.thumbnail((size, size), Image.LANCZOS)
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        buf.seek(0)
        return Response(
            content=buf.getvalue(),
            media_type="image/jpeg",
            headers={"Cache-Control": "max-age=3600"},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not generate thumbnail: {exc}")


class DeleteRequest(BaseModel):
    paths: List[str]


@app.post("/files/delete")
async def delete_files(request: DeleteRequest):
    if not request.paths:
        raise HTTPException(status_code=400, detail="No paths provided")

    results = []
    for path_str in request.paths:
        p = Path(path_str)
        try:
            if not p.exists():
                results.append({"path": path_str, "status": "not_found"})
            elif not p.is_file():
                results.append({"path": path_str, "status": "error", "reason": "Not a file"})
            else:
                p.unlink()
                results.append({"path": path_str, "status": "deleted"})
        except Exception as exc:
            results.append({"path": path_str, "status": "error", "reason": str(exc)})

    deleted = [r for r in results if r["status"] == "deleted"]
    errors  = [r for r in results if r["status"] == "error"]
    return {"deleted": len(deleted), "errors": errors, "results": results}


class MoveToBinRequest(BaseModel):
    paths: List[str]
    scan_root: str


@app.post("/files/move-to-bin")
async def move_to_bin(request: MoveToBinRequest):
    if not request.paths:
        raise HTTPException(status_code=400, detail="No paths provided")

    bin_dir = Path(request.scan_root) / "_duplicate_bin"
    bin_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for path_str in request.paths:
        p = Path(path_str)
        try:
            if not p.exists():
                results.append({"path": path_str, "status": "not_found"})
                continue
            if not p.is_file():
                results.append({"path": path_str, "status": "error", "reason": "Not a file"})
                continue
            dest = bin_dir / p.name
            if dest.exists():
                counter = 1
                while dest.exists():
                    dest = bin_dir / f"{p.stem}_{counter}{p.suffix}"
                    counter += 1
            shutil.move(str(p), str(dest))
            results.append({"path": path_str, "status": "moved", "dest": str(dest)})
        except Exception as exc:
            results.append({"path": path_str, "status": "error", "reason": str(exc)})

    moved  = [r for r in results if r["status"] == "moved"]
    errors = [r for r in results if r["status"] == "error"]
    return {"moved": len(moved), "errors": errors, "results": results, "bin": str(bin_dir)}


@app.get("/scan/{job_id}/status")
async def scan_status(job_id: str):
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "job_id": job_id,
        "status": job["status"],
        "path": job["path"],
        "stages": job["stages"],
        "progress": job["progress"],
        "error": job.get("error"),
    }
