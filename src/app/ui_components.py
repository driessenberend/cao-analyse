# ui_components.py
from __future__ import annotations

from typing import Any, Dict, Optional

import streamlit as st


def render_header() -> None:
    """
    Minimal header. Keep it static and non-interactive.
    """
    st.title("CAO Semantic Search & RAG")
    st.caption("Portfolio project: Supabase (pgvector) + OpenAI embeddings + Streamlit.")


def render_sidebar_nav() -> None:
    """
    Sidebar helpers for consistent UX.
    Note: Streamlit multipage navigation is handled by the 'pages/' folder automatically.
    This component only adds context and session controls.
    """
    with st.sidebar:
        st.subheader("Navigatie")
        st.markdown(
            "- **Search**: semantic search over chunks\n"
            "- **RAG**: analyse met bronvermelding\n"
            "- **Documents**: documentstatus + preview\n"
        )
        st.divider()
        st.subheader("Sessie")
        st.write(
            {
                "authed": st.session_state.get("authed", False),
                "last_query": st.session_state.get("last_query", ""),
                "last_cao_filter": st.session_state.get("last_cao_filter"),
            }
        )

        if st.button("Reset sessie", type="secondary"):
            # keep secrets-based auth out; reset everything including auth
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()


def render_evidence_card(source_label: str, row: Dict[str, Any]) -> None:
    """
    Standard rendering for a source chunk.
    """
    meta = (
        f"{source_label}  "
        f"cao_id={row.get('cao_id')} | chunk={row.get('chunk_index')} | "
        f"pages={row.get('page_start')}-{row.get('page_end')} | chunk_id={row.get('chunk_id')}"
    )
    st.markdown(f"**{meta}**")
    st.write((row.get("chunk_content") or "").strip())
    st.divider()


def render_search_result(row: Dict[str, Any]) -> None:
    """
    Standard rendering for search results.
    """
    dist = row.get("distance")
    dist_txt = f"{dist:.4f}" if isinstance(dist, (int, float)) else str(dist)
    meta = (
        f"cao_id={row.get('cao_id')} | chunk={row.get('chunk_index')} | "
        f"pages={row.get('page_start')}-{row.get('page_end')} | distance={dist_txt}"
    )
    st.markdown(f"**{meta}**")
    st.write((row.get("chunk_content") or "").strip())
    st.divider()


def render_kv(label: str, value: Any) -> None:
    st.markdown(f"**{label}**")
    st.write(value)