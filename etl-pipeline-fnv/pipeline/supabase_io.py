# ----------------------------- supabase I/O -----------------------------

from __future__ import annotations

from typing import List

from supabase import Client

def download_pdf_from_storage(supabase: Client, bucket: str, path: str) -> bytes:
    storage = supabase.storage.from_(bucket)
    data = storage.download(path)
    # supabase-py sometimes returns bytes directly; sometimes a response-like object
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if hasattr(data, "read"):
        return data.read()
    raise RuntimeError("Unexpected download result from supabase storage")


def iter_documents_to_process(
    supabase: Client,
    *,
    only_unprocessed: bool,
    limit: int,
) -> List[dict]:
    q = supabase.table("cao_documents").select(
        "cao_id,cao_name,storage_bucket,storage_path,processed_at"
    )
    if only_unprocessed:
        q = q.is_("processed_at", "null")
    q = q.limit(limit)
    res = q.execute()
    return res.data or []


def upsert_chunks(supabase: Client, rows: List[dict], batch: int) -> None:
    for i in range(0, len(rows), batch):
        supabase.table("cao_chunks").upsert(rows[i:i + batch]).execute()


def mark_processed(supabase: Client, cao_id: str) -> None:
    supabase.table("cao_documents").update({"processed_at": "now()"}).eq("cao_id", cao_id).execute()
