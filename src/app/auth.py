# auth.py
from __future__ import annotations

import streamlit as st


def require_password(app_title: str = "CAO App") -> None:
    """
    Simple shared-secret gate for Streamlit.

    Required in .streamlit/secrets.toml:
      [auth]
      password = "..."

    Usage (top of each page):
      from auth import require_password
      require_password("My App")
    """
    pwd = st.secrets.get("auth", {}).get("password")
    if not pwd:
        # Misconfigured secrets: do not expose app.
        st.stop()

    if st.session_state.get("authed") is True:
        return

    st.title(app_title)
    entered = st.text_input("Wachtwoord", type="password")
    if entered and entered == pwd:
        st.session_state["authed"] = True
        st.rerun()

    st.stop()