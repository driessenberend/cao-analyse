from __future__ import annotations

import streamlit as st
from supabase import Client, create_client

from core.settings import SupabaseSettings


@st.cache_resource
def get_supabase_client(settings: SupabaseSettings) -> Client:
    return create_client(settings.url, settings.anon_key)
