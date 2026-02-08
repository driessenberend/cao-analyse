# main.py
from __future__ import annotations

import streamlit as st

from auth import require_password
from state import ensure_state
from ui_components import render_header, render_sidebar_nav


def run_main() -> None:
    """
    Main entry for a multipage Streamlit app.

    Expected structure:
      - main.py (this file)
      - pages/1_Search.py, pages/2_RAG.py, pages/3_Documents.py
      - auth.py, state.py, ui_components.py
      - core/, clients/, services/ for configuration and business logic
    """
    st.set_page_config(
        page_title="CAO Semantic Search",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    ensure_state()
    require_password("CAO Semantic Search")

    render_header()
    render_sidebar_nav()


run_main()
