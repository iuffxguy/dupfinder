import os
import hashlib
from collections import defaultdict
from typing import Callable, Dict, List, Optional

try:
    import xxhash
    _HAS_XXHASH = True
except ImportError:
    _HAS_XXHASH = False

MEDIA_EXTENSIONS = {
    # Images — common
    "jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "tif",
    # Images — Apple / mobile
    "heic", "heif",
    # Images — RAW camera formats
    "raw", "cr2", "cr3", "nef", "nrw", "arw", "srf", "sr2",
    "dng", "orf", "rw2", "pef", "x3f", "raf", "rwl", "iiq",
    # Images — other
    "svg", "ico", "psd", "xcf", "ai", "eps",
    # Video — common
    "mp4", "mkv", "avi", "mov", "wmv", "flv", "m4v", "webm",
    # Video — mobile / camcorder
    "3gp", "3g2", "mts", "m2ts", "ts",
    # Video — disc / broadcast
    "vob", "mpg", "mpeg", "m2v", "mp2", "mxf", "ogv",
    # Video — other
    "f4v", "divx", "rmvb", "rm", "asf",
    # Audio — common
    "mp3", "flac", "wav", "aac", "m4a", "ogg", "opus", "wma",
    # Audio — lossless / hi-res
    "aiff", "aif", "ape", "alac", "dsf", "dff", "wv",
    # Audio — other
    "mid", "midi", "amr", "au",
}

PARTIAL_READ_BYTES = 65536   # 64 KB
MIN_FILE_SIZE      = 1024    # 1 KB
CHUNK_SIZE         = 1 << 20 # 1 MB per read chunk
DUP_BIN_NAME       = "_duplicate_bin"


def _is_media_file(path: str) -> bool:
    ext = os.path.splitext(path)[1].lstrip(".").lower()
    return ext in MEDIA_EXTENSIONS


def _new_hasher():
    return xxhash.xxh64() if _HAS_XXHASH else hashlib.md5()


def _partial_hash(path: str) -> Optional[str]:
    try:
        h = _new_hasher()
        with open(path, "rb") as f:
            h.update(f.read(PARTIAL_READ_BYTES))
        return h.hexdigest()
    except (OSError, PermissionError):
        return None


def _full_hash(path: str, progress_cb: Optional[Callable] = None) -> Optional[str]:
    """Hash entire file. Calls progress_cb(bytes_done, total_bytes) periodically."""
    try:
        total = os.path.getsize(path)
        h = _new_hasher()
        done = 0
        with open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
                done += len(chunk)
                if progress_cb and total > 0:
                    progress_cb(done, total)
        return h.hexdigest()
    except (OSError, PermissionError):
        return None


class DuplicateScanner:
    def __init__(self, callback: Optional[Callable] = None, checkpoint_callback: Optional[Callable] = None):
        """
        callback(stage, progress_pct, message, found_count)
        checkpoint_callback(stage_key, data)
        """
        self.callback = callback or (lambda *a, **kw: None)
        self.checkpoint_callback = checkpoint_callback or (lambda *a, **kw: None)

    def _emit(self, stage: str, progress: float, message: str, found: int):
        self.callback(stage, progress, message, found)

    def group_by_size(self, path: str) -> Dict[int, List[str]]:
        size_map: Dict[int, List[str]] = defaultdict(list)
        all_files: List[str] = []

        for dirpath, dirnames, filenames in os.walk(path, followlinks=False):
            # Skip the duplicate bin so we never scan files moved there
            dirnames[:] = [d for d in dirnames if d != DUP_BIN_NAME]
            for fname in filenames:
                full = os.path.join(dirpath, fname)
                if _is_media_file(full):
                    all_files.append(full)

        total = len(all_files)
        self._emit("size", 0.0, f"Found {total} media files to examine", 0)

        for idx, fpath in enumerate(all_files):
            try:
                size = os.path.getsize(fpath)
                if size >= MIN_FILE_SIZE:
                    size_map[size].append(fpath)
            except (OSError, PermissionError):
                pass

            if total > 0 and (idx % 100 == 0 or idx == total - 1):
                pct = (idx + 1) / total * 100
                self._emit("size", pct, f"Sizing files\u2026 {idx + 1}/{total}", 0)

        groups = {sz: paths for sz, paths in size_map.items() if len(paths) > 1}
        found  = sum(len(v) for v in groups.values())
        self._emit("size", 100.0, f"Size grouping complete \u2014 {len(groups)} groups ({found} files)", len(groups))
        return groups

    def group_by_partial_hash(self, size_groups: Dict[int, List[str]]) -> Dict[str, List[str]]:
        all_files: List[str] = [f for paths in size_groups.values() for f in paths]
        total = len(all_files)
        self._emit("partial", 0.0, f"Partial-hashing {total} candidate files", 0)

        hash_map: Dict[str, List[str]] = defaultdict(list)
        for idx, fpath in enumerate(all_files):
            h = _partial_hash(fpath)
            if h is not None:
                hash_map[h].append(fpath)

            if total > 0 and (idx % 20 == 0 or idx == total - 1):
                pct = (idx + 1) / total * 100
                self._emit("partial", pct, f"Partial hash\u2026 {idx + 1}/{total}", 0)

        groups = {h: paths for h, paths in hash_map.items() if len(paths) > 1}
        found  = sum(len(v) for v in groups.values())
        self._emit("partial", 100.0, f"Partial hash complete \u2014 {len(groups)} groups ({found} files)", len(groups))
        return groups

    def group_by_full_hash(
        self,
        partial_groups: Dict[str, List[str]],
        mid_stage_state: Optional[dict] = None,
    ) -> Dict[str, List[str]]:
        all_files: List[str] = [f for paths in partial_groups.values() for f in paths]
        total_files = len(all_files)

        # Resume from mid-stage checkpoint if available
        if mid_stage_state:
            hash_map: Dict[str, List[str]] = defaultdict(list)
            for h, paths in mid_stage_state.get("hash_map", {}).items():
                hash_map[h].extend(paths)
            already_done: set = set(mid_stage_state.get("processed_paths", []))
            remaining = [f for f in all_files if f not in already_done]
            done_count = len(already_done)
            self._emit("full", done_count / total_files * 100,
                       f"Resuming full hash from file {done_count + 1}/{total_files}", 0)
        else:
            hash_map = defaultdict(list)
            already_done = set()
            remaining = all_files
            done_count = 0
            self._emit("full", 0.0, f"Full-hashing {total_files} candidate files", 0)

        MID_CHECKPOINT_EVERY = 100  # save progress every N files

        for step, fpath in enumerate(remaining):
            global_idx = done_count + step
            fname = os.path.basename(fpath)

            def _file_progress(bytes_done: int, bytes_total: int, _idx=global_idx, _name=fname):
                file_pct = bytes_done / bytes_total
                overall  = (_idx + file_pct) / total_files * 100
                self._emit("full", overall,
                           f"Full hash\u2026 file {_idx + 1}/{total_files}: {_name} ({int(file_pct*100)}%)", 0)

            h = _full_hash(fpath, progress_cb=_file_progress)
            if h is not None:
                hash_map[h].append(fpath)

            pct = (global_idx + 1) / total_files * 100
            self._emit("full", pct, f"Full hash\u2026 {global_idx + 1}/{total_files}", 0)

            # Save mid-stage checkpoint every N files
            if (step + 1) % MID_CHECKPOINT_EVERY == 0:
                processed_so_far = list(already_done) + remaining[: step + 1]
                self.checkpoint_callback("full_hash_partial", {
                    "hash_map":        dict(hash_map),
                    "processed_paths": processed_so_far,
                    "total":           total_files,
                    "done":            global_idx + 1,
                })

        groups = {h: paths for h, paths in hash_map.items() if len(paths) > 1}
        found  = sum(len(v) for v in groups.values())
        self._emit("full", 100.0,
                   f"Full hash complete \u2014 {len(groups)} confirmed duplicate groups ({found} files)",
                   len(groups))
        return groups

    def scan(self, path: str, stages: List[str], checkpoints: dict = None) -> Dict:
        if checkpoints is None:
            checkpoints = {}

        size_groups:    Dict[int, List[str]] = {}
        partial_groups: Dict[str, List[str]] = {}
        full_groups:    Dict[str, List[str]] = {}

        # Determine which stages to skip based on available checkpoints
        has_partial_checkpoint = "partial_groups" in checkpoints
        has_size_checkpoint    = "size_groups" in checkpoints

        if has_partial_checkpoint:
            # Both size and partial stages can be skipped
            partial_groups = checkpoints["partial_groups"]
            self._emit("size", 100.0, "Resumed from checkpoint (size stage skipped)", 0)
            self._emit("partial", 100.0, "Resumed from checkpoint (partial hash stage skipped)", len(partial_groups))
        elif has_size_checkpoint:
            # Only the size stage can be skipped; convert string keys back to int
            raw = checkpoints["size_groups"]
            size_groups = {int(k): v for k, v in raw.items()}
            self._emit("size", 100.0, "Resumed from checkpoint (size stage skipped)", len(size_groups))

            if "partial" in stages and size_groups:
                partial_groups = self.group_by_partial_hash(size_groups)
                self.checkpoint_callback("partial_groups", partial_groups)
            elif "partial" in stages:
                self._emit("partial", 100.0, "No candidates from size stage", 0)
            else:
                self._emit("partial", 100.0, "Skipped", 0)
        else:
            # No checkpoints — run normally
            if "size" in stages:
                size_groups = self.group_by_size(path)
                self.checkpoint_callback("size_groups", {str(k): v for k, v in size_groups.items()})
            else:
                self._emit("size", 100.0, "Skipped", 0)

            if "partial" in stages and size_groups:
                partial_groups = self.group_by_partial_hash(size_groups)
                self.checkpoint_callback("partial_groups", partial_groups)
            elif "partial" in stages:
                self._emit("partial", 100.0, "No candidates from size stage", 0)
            else:
                self._emit("partial", 100.0, "Skipped", 0)

        # Full hash stage — always runs from whatever partial_groups we have
        mid_stage = checkpoints.get("full_hash_partial")
        if "full" in stages and partial_groups:
            full_groups = self.group_by_full_hash(partial_groups, mid_stage_state=mid_stage)
        elif "full" in stages and size_groups and "partial" not in stages:
            full_groups = self.group_by_full_hash(
                {str(sz): paths for sz, paths in size_groups.items()},
                mid_stage_state=mid_stage,
            )
        elif "full" in stages:
            self._emit("full", 100.0, "No candidates to full-hash", 0)
        else:
            self._emit("full", 100.0, "Skipped", 0)

        if "full" in stages:
            final_groups = full_groups
        elif "partial" in stages:
            final_groups = partial_groups
        else:
            final_groups = {str(sz): paths for sz, paths in size_groups.items()}

        return self._build_results(final_groups, path)

    def _build_results(self, groups: Dict[str, List[str]], root: str) -> Dict:
        duplicate_groups = []
        total_wasted = 0

        for group_hash, paths in groups.items():
            if not paths:
                continue
            try:
                file_size = os.path.getsize(paths[0])
            except OSError:
                file_size = 0

            wasted = (len(paths) - 1) * file_size
            files  = []
            for p in paths:
                try:
                    stat = os.stat(p)
                    files.append({
                        "path":      p,
                        "name":      os.path.basename(p),
                        "directory": os.path.dirname(p),
                        "size":      stat.st_size,
                        "modified":  stat.st_mtime,
                    })
                except OSError:
                    files.append({
                        "path":      p,
                        "name":      os.path.basename(p),
                        "directory": os.path.dirname(p),
                        "size":      file_size,
                        "modified":  0,
                    })

            duplicate_groups.append({
                "hash":        group_hash,
                "file_size":   file_size,
                "file_count":  len(paths),
                "wasted_bytes": wasted,
                "files":       files,
            })
            total_wasted += wasted

        total_files = sum(g["file_count"] for g in duplicate_groups)
        return {
            "group_count":        len(duplicate_groups),
            "total_files":        total_files,
            "total_wasted_bytes": total_wasted,
            "groups":             duplicate_groups,
        }
