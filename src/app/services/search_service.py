from __future__ import annotations

from typing import Any, Dict, List, Optional

from openai import OpenAI
from supabase import Client


class SearchService:
    def __init__(
        self,
        supabase: Client,
        openai_client: OpenAI,
        *,
        embedding_model: str,
        rpc_match_fn: str,
    ) -> None:
        self._supabase = supabase
        self._openai = openai_client
        self._embedding_model = embedding_model
        self._rpc_match_fn = rpc_match_fn

    def list_documents(self, limit: int = 1000) -> List[Dict[str, Any]]:
        res = (
            self._supabase.table("cao_documents")
            .select("cao_id,cao_name,processed_at")
            .order("cao_name")
            .limit(limit)
            .execute()
        )
        return res.data or []

    def embed_query(self, text: str) -> List[float]:
        resp = self._openai.embeddings.create(model=self._embedding_model, input=text)
        return resp.data[0].embedding

    def match_chunks(
        self,
        *,
        query_embedding: List[float],
        k: int,
        cao_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        payload = {"query_embedding": query_embedding, "match_count": k, "filter_cao_id": cao_id}
        res = self._supabase.rpc(self._rpc_match_fn, payload).execute()
        return res.data or []
