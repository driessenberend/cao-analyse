from __future__ import annotations

import streamlit as st
from openai import OpenAI

from core.settings import OpenAISettings


@st.cache_resource
def get_openai_client(settings: OpenAISettings) -> OpenAI:
    return OpenAI(api_key=settings.api_key)
