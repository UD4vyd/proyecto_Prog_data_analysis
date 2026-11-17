"""Bronze layer: file naming, raw saving, watermarks and ready markers.

Rules from docs/ingestion_contract.md:
- the response body is written byte for byte (response.content) before anything parses it;
- a .headers.json file next to it keeps url, status, retrieved_utc and the HTTP headers;
- a Bronze file is never modified or overwritten.

A data file is complete only when its .headers.json exists, because we write that file second.
Silver only reads complete files.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.config import bronze_root

log = logging.getLogger(__name__)

HEADERS_SUFFIX = ".headers.json"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp(moment: datetime | None = None) -> str:
    return (moment or utc_now()).strftime("%Y%m%d_%H%M%S")


def headers_path(data_path: Path) -> Path:
    return data_path.with_name(data_path.name + HEADERS_SUFFIX)


def bronze_path(folder: str, prefix: str, ext: str, stamp: str, root: Path | None = None) -> Path:
    """datalake_bronze/<folder>/<prefix>_<YYYYMMDD_HHMMSS>.<ext>"""
    return (root or bronze_root()) / folder / f"{prefix}_{stamp}.{ext}"


def save_raw(response: requests.Response, folder: str, prefix: str, ext: str,
             root: Path | None = None) -> Path:
    """Save the body exactly as received, then its .headers.json. Never overwrite a file."""
    path = bronze_path(folder, prefix, ext, utc_stamp(), root)
    while path.exists():  # two requests in the same second: wait for a new stamp
        time.sleep(1)
        path = bronze_path(folder, prefix, ext, utc_stamp(), root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    meta = {
        "url": response.url,
        "status": response.status_code,
        "retrieved_utc": utc_now().isoformat(timespec="seconds"),
        "headers": dict(response.headers),
    }
    headers_path(path).write_text(json.dumps(meta, indent=1), encoding="utf-8")
    log.info("saved %s status=%s bytes=%d", path.name, response.status_code, len(response.content))
    return path


def read_headers(data_path: Path) -> dict:
    return json.loads(headers_path(data_path).read_text(encoding="utf-8"))


def is_complete(data_path: Path) -> bool:
    return headers_path(data_path).exists()


# Watermarks ---------------------------------------------------------------------------------

def watermarks_path(root: Path | None = None) -> Path:
    return (root or bronze_root()) / "_state" / "watermarks.json"


def read_watermarks(root: Path | None = None) -> dict:
    path = watermarks_path(root)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, data: dict) -> None:
    """Write to a temporary file and rename it, so a reader never sees half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def write_watermarks(watermarks: dict, root: Path | None = None) -> Path:
    path = watermarks_path(root)
    _write_json_atomic(path, watermarks)
    return path


# Ready markers --------------------------------------------------------------------------------

def ready_dir(root: Path | None = None) -> Path:
    return (root or bronze_root()) / "_ready"


def write_ready_marker(files: list[str], producer: str, root: Path | None = None) -> Path:
    """Signal for the Silver FileSensor: a Bronze run finished and saved these files."""
    stamp = utc_stamp()
    path = ready_dir(root) / f"bronze_ready_{stamp}.json"
    _write_json_atomic(path, {"producer": producer, "created_utc": utc_now().isoformat(timespec="seconds"),
                              "files": sorted(files)})
    return path


def list_ready_markers(root: Path | None = None) -> list[Path]:
    folder = ready_dir(root)
    return sorted(folder.glob("bronze_ready_*.json")) if folder.exists() else []


def relative_to_bronze(path: Path, root: Path | None = None) -> str:
    return path.resolve().relative_to((root or bronze_root()).resolve()).as_posix()
