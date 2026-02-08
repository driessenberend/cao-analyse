# search.py
from __future__ import annotations

import os
from typing import Any, Dict, List

import streamlit as st

from clients.openai_client import get_openai_client
from clients.supabase_client import get_supabase_client
from core.errors import MissingConfigError
from core.settings import load_settings, require_openai, require_supabase
from services.search_service import SearchService

def render_result(row: Dict[str, Any]) -> None:
    dist = row.get("distance")
    dist_txt = f"{dist:.4f}" if isinstance(dist, (int, float)) else str(dist)
    meta = (
        f"cao_id={row.get('cao_id')} | chunk={row.get('chunk_index')} | "
        f"pages={row.get('page_start')}-{row.get('page_end')} | "
        f"distance={dist_txt}"
    )
    st.markdown(f"**{meta}**")
    st.write(row.get("chunk_content", ""))
    st.divider()


def run_search_page() -> None:
    try:
        settings = load_settings(st.secrets, os.environ)
        require_supabase(settings.supabase)
        require_openai(settings.openai)
    except MissingConfigError as exc:
        st.error(str(exc))
        st.stop()

    supabase = get_supabase_client(settings.supabase)
    oai = get_openai_client(settings.openai)
    service = SearchService(
        supabase,
        oai,
        embedding_model=settings.openai.embedding_model,
        rpc_match_fn=settings.rpc_match_fn,
    )

    st.title("Search")
    st.caption("Semantic search over CAO chunks (pgvector in Supabase).")

    with st.sidebar:
        st.subheader("Filters")
        docs = service.list_documents()
        options = [("Alle CAO's", None)] + [(f"{d['cao_name']} ({d['cao_id']})", d["cao_id"]) for d in docs]
        label_to_id = {label: cid for label, cid in options}
        selected_label = st.selectbox("CAO", [x[0] for x in options], index=0)
        filter_cao_id = label_to_id[selected_label]
        k = st.slider("Top K", min_value=5, max_value=50, value=12, step=1)

    query = st.text_input("Zoekterm", placeholder="Bijv: overwerk toeslag, loonsverhoging, reiskosten, verlof")
    if not query:
        st.stop()

    qvec = service.embed_query(query)
    rows = service.match_chunks(query_embedding=qvec, k=k, cao_id=filter_cao_id)

    st.subheader("Resultaten")
    if not rows:
        st.info("Geen resultaten.")
        st.stop()

    for r in rows:
        render_result(r)
