# state.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import streamlit as st


@dataclass(frozen=True)
class AppState:
    """
    Centralized session state keys (single source of truth).
    """
    authed: bool = False
    last_query: str = ""
    last_cao_filter: Optional[str] = None
    last_results: Optional[list] = None
    last_rag_question: str = ""
    last_rag_answer: str = ""
    created_at: float = 0.0


def ensure_state() -> None:
    """
    Initialize session_state keys in a controlled way.
    Call this once at app start (main.py) and optionally on each page.
    """
    defaults: Dict[str, Any] = {
        "authed": False,
        "created_at": time.time(),
        "last_query": "",
        "last_cao_filter": None,
        "last_results": None,
        "last_rag_question": "",
        "last_rag_answer": "",
        # UI toggles
        "show_sources": True,
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def set_last_search(query: str, cao_id: Optional[str], results: list) -> None:
    st.session_state["last_query"] = query
    st.session_state["last_cao_filter"] = cao_id
    st.session_state["last_results"] = results


def set_last_rag(question: str, answer: str) -> None:
    st.session_state["last_rag_question"] = question
    st.session_state["last_rag_answer"] = answer