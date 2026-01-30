# search.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import streamlit as st
from openai import OpenAI
from supabase import create_client, Client


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_anon_key: str
    openai_api_key: str
    embedding_model: str
    rpc_match_fn: str

    @staticmethod
    def load() -> "Settings":
        supa = st.secrets.get("supabase", {})
        oai = st.secrets.get("openai", {})

        return Settings(
            supabase_url=supa.get("url") or os.environ["SUPABASE_URL"],
            supabase_anon_key=supa.get("anon_key") or os.environ["SUPABASE_ANON_KEY"],
            openai_api_key=oai.get("api_key") or os.environ["OPENAI_API_KEY"],
            embedding_model="text-embedding-3-small",
            rpc_match_fn="match_cao_chunks",
        )


@st.cache_resource
def get_supabase(settings: Settings) -> Client:
    return create_client(settings.supabase_url, settings.supabase_anon_key)


@st.cache_resource
def get_openai(settings: Settings) -> OpenAI:
    return OpenAI(api_key=settings.openai_api_key)


def embed_query(oai: OpenAI, model: str, text: str) -> List[float]:
    resp = oai.embeddings.create(model=model, input=text)
    return resp.data[0].embedding


def fetch_documents(supabase: Client, limit: int = 1000) -> List[Dict[str, Any]]:
    res = (
        supabase.table("cao_documents")
        .select("cao_id,cao_name,processed_at")
        .order("cao_name")
        .limit(limit)
        .execute()
    )
    return res.data or []


def match_chunks(
    supabase: Client,
    rpc_fn: str,
    *,
    query_embedding: List[float],
    k: int,
    cao_id: Optional[str],
) -> List[Dict[str, Any]]:
    payload = {"query_embedding": query_embedding, "match_count": k, "filter_cao_id": cao_id}
    res = supabase.rpc(rpc_fn, payload).execute()
    return res.data or []


def render_result(row: Dict[str, Any]) -> None:
    meta = (
        f"cao_id={row.get('cao_id')} | chunk={row.get('chunk_index')} | "
        f"pages={row.get('page_start')}-{row.get('page_end')} | "
        f"distance={row.get('distance'):.4f}"
    )
    st.markdown(f"**{meta}**")
    st.write(row.get("chunk_content", ""))
    st.divider()


def run_search_page() -> None:
    settings = Settings.load()
    supabase = get_supabase(settings)
    oai = get_openai(settings)

    st.title("Search")
    st.caption("Semantic search over CAO chunks (pgvector in Supabase).")

    with st.sidebar:
        st.subheader("Filters")
        docs = fetch_documents(supabase)
        options = [("Alle CAO's", None)] + [(f"{d['cao_name']} ({d['cao_id']})", d["cao_id"]) for d in docs]
        label_to_id = {label: cid for label, cid in options}
        selected_label = st.selectbox("CAO", [x[0] for x in options], index=0)
        filter_cao_id = label_to_id[selected_label]
        k = st.slider("Top K", min_value=5, max_value=50, value=12, step=1)

    query = st.text_input("Zoekterm", placeholder="Bijv: overwerk toeslag, loonsverhoging, reiskosten, verlof")
    if not query:
        st.stop()

    qvec = embed_query(oai, settings.embedding_model, query)
    rows = match_chunks(supabase, settings.rpc_match_fn, query_embedding=qvec, k=k, cao_id=filter_cao_id)

    st.subheader("Resultaten")
    if not rows:
        st.info("Geen resultaten.")
        st.stop()

    for r in rows:
        render_result(r)