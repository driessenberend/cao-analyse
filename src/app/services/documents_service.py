from __future__ import annotations

from typing import Any, Dict, List, Optional

from supabase import Client


class DocumentsService:
    def __init__(self, supabase: Client) -> None:
        self._supabase = supabase

    def list_documents(self, limit: int = 2000) -> List[Dict[str, Any]]:
        res = (
            self._supabase.table("cao_documents")
            .select("cao_id,cao_name,source_url,storage_bucket,storage_path,file_sha256,file_bytes,processed_at,ingested_at")
            .order("cao_name")
            .limit(limit)
            .execute()
        )
        return res.data or []

    def chunk_count(self, cao_id: str) -> int:
        res = (
            self._supabase.table("cao_chunks")
            .select("chunk_id", count="exact")
            .eq("cao_id", cao_id)
            .limit(1)
            .execute()
        )
        return int(res.count or 0)

    def chunk_preview(self, cao_id: str, limit: int) -> List[Dict[str, Any]]:
        return (
            self._supabase.table("cao_chunks")
            .select("chunk_id,chunk_index,page_start,page_end,chunk_content")
            .eq("cao_id", cao_id)
            .order("chunk_index")
            .limit(limit)
            .execute()
            .data
            or []
        )

    def get_pdf_url(self, bucket: str, path: str) -> Optional[str]:
        storage = self._supabase.storage.from_(bucket)

        try:
            pub = storage.get_public_url(path)
            if isinstance(pub, dict):
                url = pub.get("publicUrl") or pub.get("data", {}).get("publicUrl")
                if url:
                    return url
            if isinstance(pub, str):
                return pub
        except Exception:
            pass

        try:
            signed = storage.create_signed_url(path, 60 * 60)
            if isinstance(signed, dict):
                url = signed.get("signedUrl") or signed.get("data", {}).get("signedUrl")
                if url:
                    return url
            if isinstance(signed, str):
                return signed
        except Exception:
            pass

        return None
