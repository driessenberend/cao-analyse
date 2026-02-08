from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from openai import OpenAI
from supabase import Client


class RagService:
    def __init__(
        self,
        supabase: Client,
        openai_client: OpenAI,
        *,
        embedding_model: str,
        chat_model: str,
        rpc_match_fn: str,
    ) -> None:
        self._supabase = supabase
        self._openai = openai_client
        self._embedding_model = embedding_model
        self._chat_model = chat_model
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

    @staticmethod
    def build_sources_context(rows: List[Dict[str, Any]], max_chars: int = 12000) -> str:
        parts: List[str] = []
        used = 0
        for i, r in enumerate(rows, 1):
            label = f"S{i}"
            header = (
                f"[{label}] "
                f"cao_id={r.get('cao_id')} | chunk_id={r.get('chunk_id')} | chunk_index={r.get('chunk_index')} | "
                f"pages={r.get('page_start')}-{r.get('page_end')}"
            )
            body = (r.get("chunk_content") or "").strip()
            block = header + "\n" + body + "\n"
            if used + len(block) > max_chars:
                break
            parts.append(block)
            used += len(block)
        return "\n".join(parts)

    @staticmethod
    def citation_check(text: str, n_sources: int) -> bool:
        if "[S" not in text:
            return False
        for m in re.findall(r"\[S(\d+)\]", text):
            idx = int(m)
            if idx < 1 or idx > n_sources:
                return False
        return True

    def generate_answer(self, question: str, system_rules: str, sources: str) -> str:
        messages = [
            {"role": "system", "content": system_rules},
            {
                "role": "user",
                "content": (
                    "Vraag:\n"
                    f"{question}\n\n"
                    "Bronnen:\n"
                    f"{sources}\n\n"
                    "Schrijf een analyse met claims die elk bronverwijzingen bevatten als [S1], [S2], etc.\n"
                    "Geen claims zonder bronverwijzing. Als bronnen onvoldoende zijn: zeg dat expliciet."
                ),
            },
        ]
        resp = self._openai.chat.completions.create(
            model=self._chat_model,
            messages=messages,
            temperature=0.2,
        )
        return resp.choices[0].message.content or ""
