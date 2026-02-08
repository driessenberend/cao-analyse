from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from supabase import Client

from pipeline.config import IngestSettings


def slugify(value: str) -> str:
    keep = []
    for ch in value.lower():
        if ch.isalnum():
            keep.append(ch)
        elif ch in ("-", "_", " "):
            keep.append("-")
    slug = "".join(keep)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "cao"


def read_manifest(path: Path) -> List[dict]:
    if not path.exists():
        return []
    rows: List[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def iter_local_files(data_dir: Path) -> Iterable[Path]:
    for entry in data_dir.iterdir():
        if entry.is_file() and entry.suffix.lower() == ".pdf":
            yield entry


def build_records(manifest_rows: List[dict], data_dir: Path) -> List[dict]:
    if manifest_rows:
        return manifest_rows
    return [
        {
            "file_name": path.name,
            "source_url": None,
            "pdf_url": None,
            "cao_name": path.stem,
        }
        for path in iter_local_files(data_dir)
    ]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def upload_pdf(
    supabase: Client,
    bucket: str,
    storage_path: str,
    data: bytes,
) -> None:
    storage = supabase.storage.from_(bucket)
    res = storage.upload(
        storage_path,
        data,
        {"content-type": "application/pdf", "x-upsert": "true"},
    )
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(res["error"])


def ingest_documents(supabase: Client, settings: IngestSettings) -> int:
    if not settings.data_dir.exists():
        return 0
    manifest_rows = build_records(read_manifest(settings.manifest_path), settings.data_dir)
    rows: List[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for item in manifest_rows:
        file_name = item.get("file_name")
        if not file_name:
            continue
        path = settings.data_dir / file_name
        if not path.exists():
            continue
        file_bytes = path.read_bytes()
        file_sha = sha256_bytes(file_bytes)
        cao_name = item.get("cao_name") or path.stem
        cao_id = slugify(cao_name)
        storage_path = f"{cao_id}/{file_name}"

        upload_pdf(supabase, settings.storage_bucket, storage_path, file_bytes)

        rows.append(
            {
                "cao_id": cao_id,
                "cao_name": cao_name,
                "source_url": item.get("source_url"),
                "storage_bucket": settings.storage_bucket,
                "storage_path": storage_path,
                "file_sha256": file_sha,
                "file_bytes": len(file_bytes),
                "ingested_at": now,
            }
        )

    if rows:
        supabase.table("cao_documents").upsert(rows, on_conflict="cao_id").execute()

    return len(rows)
