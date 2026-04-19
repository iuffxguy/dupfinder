import asyncio
import io
import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.scanner import DuplicateScanner

app = FastAPI(title="DupFinder", version="1.0.0")

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── Database ─────────────────────────────────────────────────────────────────
def _resolve_data_dir() -> Path:
    preferred = Path(os.environ.get("DUPFINDER_DATA", "/data/.dupfinder"))
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        # Verify it's actually writable
        test = preferred / ".write_test"
        test.touch()
        test.unlink()
        return preferred
    except Exception:
        # Fall back to a temp directory so the server always starts
        import tempfile
        fallback = Path(tempfile.gettempdir()) / "dupfinder"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback

DATA_DIR = _resolve_data_dir()
DB_PATH  = DATA_DIR / "jobs.db"
_db_lock = threading.Lock()


def _db_conn():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _db_lock:
        conn = _db_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id          TEXT PRIMARY KEY,
                status      TEXT NOT NULL DEFAULT 'running',
                path        TEXT NOT NULL,
                stages      TEXT NOT NULL,
                created_at  REAL NOT NULL,
                updated_at  REAL NOT NULL,
                progress    TEXT DEFAULT '{}',
                checkpoints TEXT DEFAULT '{}',
                results     TEXT,
                error       TEXT
            )
        """)
        conn.commit()
        conn.execute(
            "UPDATE jobs SET status='interrupted', error='Server was restarted' WHERE status='running'"
        )
        conn.commit()
        conn.close()


def db_create_job(job_id: str, path: str, stages: List[str]):
    now = time.time()
    with _db_lock:
        conn = _db_conn()
        conn.execute(
            "INSERT INTO jobs (id,status,path,stages,created_at,updated_at,checkpoints) VALUES (?,?,?,?,?,?,?)",
            (job_id, "running", path, json.dumps(stages), now, now, "{}"),
        )
        conn.commit()
        conn.close()


def db_update_job(job_id: str, **fields):
    if not fields:
        return
    fields["updated_at"] = time.time()
    for k, v in list(fields.items()):
        if isinstance(v, (dict, list)):
            fields[k] = json.dumps(v)
    with _db_lock:
        conn = _db_conn()
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(
            f"UPDATE jobs SET {set_clause} WHERE id = ?",
            list(fields.values()) + [job_id],
        )
        conn.commit()
        conn.close()


def _parse_job(row) -> dict:
    d = dict(row)
    for field in ("progress", "checkpoints", "stages"):
        if d.get(field):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                pass
    if d.get("results"):
        try:
            d["results"] = json.loads(d["results"])
        except Exception:
            pass
    return d


def db_get_job(job_id: str) -> Optional[dict]:
    with _db_lock:
        conn = _db_conn()
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        conn.close()
    return _parse_job(row) if row else None


def db_get_recent_jobs(limit: int = 30) -> List[dict]:
    with _db_lock:
        conn = _db_conn()
        rows = conn.execute(
            "SELECT id,status,path,stages,created_at,updated_at,progress,error FROM jobs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
    result = []
    for row in rows:
        d = dict(row)
        for field in ("progress", "stages"):
            if d.get(field):
                try:
                    d[field] = json.loads(d[field])
                except Exception:
                    pass
        result.append(d)
    return result


def db_delete_job(job_id: str):
    with _db_lock:
        conn = _db_conn()
        conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()
        conn.close()


# ── In-memory live state ──────────────────────────────────────────────────────
jobs: Dict[str, dict] = {}
executor = ThreadPoolExecutor(max_workers=2)
VALID_STAGES = {"size", "partial", "full"}


# ── Startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    init_db()


# ── Models ────────────────────────────────────────────────────────────────────
class ScanRequest(BaseModel):
    path: str
    stages: List[str] = ["size", "partial", "full"]

class DeleteRequest(BaseModel):
    paths: List[str]

class MoveToBinRequest(BaseModel):
    paths: List[str]
    scan_root: str


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(index))


@app.get("/jobs")
async def list_jobs():
    return db_get_recent_jobs()


@app.delete("/jobs/{job_id}")
async def delete_job_route(job_id: str):
    jobs.pop(job_id, None)
    db_delete_job(job_id)
    return {"ok": True}


@app.post("/scan")
async def start_scan(request: ScanRequest):
    if not request.path or not request.path.strip():
        raise HTTPException(status_code=400, detail="path must not be empty")
    requested_stages = [s.lower() for s in request.stages]
    invalid = set(requested_stages) - VALID_STAGES
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid stages: {invalid}")
    if not requested_stages:
        raise HTTPException(status_code=400, detail="At least one stage must be selected")

    job_id = str(uuid.uuid4())
    _init_job_memory(job_id, request.path, requested_stages)
    db_create_job(job_id, request.path, requested_stages)

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_scan, job_id, request.path, requested_stages, {})
    return {"job_id": job_id}


@app.post("/scan/{job_id}/resume")
async def resume_scan(job_id: str):
    db_job = db_get_job(job_id)
    if not db_job:
        raise HTTPException(status_code=404, detail="Job not found")
    if db_job["status"] == "running":
        raise HTTPException(status_code=409, detail="Job is already running")

    path   = db_job["path"]
    stages = db_job["stages"] if isinstance(db_job["stages"], list) else json.loads(db_job["stages"])
    checkpoints = db_job.get("checkpoints") or {}
    if isinstance(checkpoints, str):
        try:
            checkpoints = json.loads(checkpoints)
        except Exception:
            checkpoints = {}

    _init_job_memory(job_id, path, stages)
    db_update_job(job_id, status="running", error=None)

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_scan, job_id, path, stages, checkpoints)
    return {"job_id": job_id}


def _init_job_memory(job_id: str, path: str, stages: List[str]):
    jobs[job_id] = {
        "status": "running",
        "path": path,
        "stages": stages,
        "progress": {"stage": "starting", "progress": 0, "found": 0, "message": "Initializing scan\u2026", "files_scanned": 0},
        "results": None,
        "error": None,
    }


def _run_scan(job_id: str, path: str, stages: List[str], checkpoints: dict):
    if job_id not in jobs:
        return
    last_db_sync = [time.time()]

    def progress_callback(stage, progress_pct, message, found_count):
        if job_id not in jobs:
            return
        jobs[job_id]["progress"] = {
            "stage": stage, "progress": round(progress_pct, 1),
            "found": found_count, "message": message, "files_scanned": 0,
        }
        now = time.time()
        if now - last_db_sync[0] > 5:
            db_update_job(job_id, progress=jobs[job_id]["progress"])
            last_db_sync[0] = now

    def checkpoint_callback(stage_key: str, data: dict):
        db_job = db_get_job(job_id)
        existing = db_job.get("checkpoints") or {} if db_job else {}
        if isinstance(existing, str):
            try:
                existing = json.loads(existing)
            except Exception:
                existing = {}
        existing[stage_key] = data
        db_update_job(job_id, checkpoints=existing)

    try:
        scanner = DuplicateScanner(callback=progress_callback, checkpoint_callback=checkpoint_callback)
        results = scanner.scan(path, stages, checkpoints=checkpoints)
        results["scan_root"] = path
        jobs[job_id]["results"] = results
        jobs[job_id]["status"] = "done"
        jobs[job_id]["progress"] = {
            "stage": "done", "progress": 100, "found": results["group_count"],
            "message": f"Scan complete \u2014 {results['group_count']} duplicate groups found",
            "files_scanned": results["total_files"],
        }
        db_update_job(job_id, status="done", progress=jobs[job_id]["progress"], results=results)
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)
        jobs[job_id]["progress"] = {"stage": "error", "progress": 0, "found": 0, "message": f"Error: {exc}", "files_scanned": 0}
        db_update_job(job_id, status="error", error=str(exc), progress=jobs[job_id]["progress"])


@app.get("/scan/{job_id}/progress")
async def scan_progress(job_id: str):
    if job_id not in jobs:
        db_job = db_get_job(job_id)
        if not db_job:
            raise HTTPException(status_code=404, detail="Job not found")
        jobs[job_id] = {
            "status": db_job["status"], "path": db_job["path"],
            "stages": db_job["stages"], "progress": db_job.get("progress") or {},
            "results": db_job.get("results"), "error": db_job.get("error"),
        }

    async def event_generator():
        while True:
            job = jobs.get(job_id)
            if not job:
                break
            payload = dict(job["progress"])
            payload["status"] = job["status"]
            yield f"data: {json.dumps(payload)}\n\n"
            if job["status"] in ("done", "error", "interrupted"):
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/scan/{job_id}/results")
async def scan_results(job_id: str):
    job = jobs.get(job_id)
    if not job:
        db_job = db_get_job(job_id)
        if not db_job:
            raise HTTPException(status_code=404, detail="Job not found")
        job = db_job
    if job["status"] == "running":
        raise HTTPException(status_code=202, detail="Scan still in progress")
    if job["status"] in ("error", "interrupted"):
        raise HTTPException(status_code=500, detail=job.get("error") or job["status"])
    return job["results"]


@app.get("/scan/{job_id}/status")
async def scan_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        db_job = db_get_job(job_id)
        if not db_job:
            raise HTTPException(status_code=404, detail="Job not found")
        job = db_job
    return {"job_id": job_id, "status": job["status"], "path": job.get("path"),
            "stages": job.get("stages"), "progress": job.get("progress"), "error": job.get("error")}


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
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".m4v", ".webm",
                    ".3gp", ".3g2", ".mts", ".m2ts", ".ts", ".vob", ".mpg", ".mpeg", ".ogv", ".f4v"}


def _pil_to_jpeg_response(img, size: int) -> Response:
    from PIL import Image
    img.thumbnail((size, size), Image.LANCZOS)
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    buf.seek(0)
    return Response(content=buf.getvalue(), media_type="image/jpeg",
                    headers={"Cache-Control": "max-age=3600"})


@app.get("/thumbnail")
async def get_thumbnail(path: str = Query(...), size: int = Query(200)):
    import subprocess
    from PIL import Image

    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    if suffix in IMAGE_EXTENSIONS:
        try:
            img = Image.open(str(file_path))
            return _pil_to_jpeg_response(img, size)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Could not generate thumbnail: {exc}")

    if suffix in VIDEO_EXTENSIONS:
        # Try seeking to 10s first; fall back to first frame if video is shorter
        for seek in ("00:00:10", "00:00:00"):
            try:
                result = subprocess.run(
                    ["ffmpeg", "-ss", seek, "-i", str(file_path),
                     "-vframes", "1", "-f", "image2pipe", "-vcodec", "mjpeg",
                     "-loglevel", "quiet", "-"],
                    capture_output=True, timeout=30,
                )
                if result.returncode == 0 and result.stdout:
                    img = Image.open(io.BytesIO(result.stdout))
                    return _pil_to_jpeg_response(img, size)
            except (subprocess.TimeoutExpired, Exception):
                continue
        raise HTTPException(status_code=500, detail="Could not extract video frame")

    raise HTTPException(status_code=400, detail="Not a supported file type")


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
