# rag.py
from __future__ import annotations

import os
import re
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
    chat_model: str
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
            chat_model="gpt-4.1-mini",
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


DEFAULT_SYSTEM = """Je bent een analyst die uitsluitend conclusies trekt uit de aangeleverde bronnen.
Regels:
- Elke claim krijgt één of meer bronverwijzingen tussen haakjes, bijv. [S1] of [S2][S4].
- Als een claim niet direct uit bronnen volgt: schrijf die claim niet.
- Vat niet algemeen samen; verwijs naar concrete passages.
- Bij onduidelijkheid of ontbreken van bewijs: benoem beperking.
Output:
- Korte conclusie
- Genummerde claims (met citations)
- Beperkingen
"""


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


def citation_check(text: str, n_sources: int) -> bool:
    if "[S" not in text:
        return False
    for m in re.findall(r"\[S(\d+)\]", text):
        idx = int(m)
        if idx < 1 or idx > n_sources:
            return False
    return True


def run_rag_page() -> None:
    settings = Settings.load()
    supabase = get_supabase(settings)
    oai = get_openai(settings)

    st.title("RAG")
    st.caption("Analyse over CAO-teksten met bronvermelding.")

    with st.sidebar:
        st.subheader("Scope")
        docs = fetch_documents(supabase)
        options = [("Alle CAO's", None)] + [(f"{d['cao_name']} ({d['cao_id']})", d["cao_id"]) for d in docs]
        label_to_id = {label: cid for label, cid in options}
        selected_label = st.selectbox("CAO", [x[0] for x in options], index=0)
        filter_cao_id = label_to_id[selected_label]

        k = st.slider("Context chunks", min_value=6, max_value=40, value=16, step=1)
        system_rules = st.text_area("System rules", value=DEFAULT_SYSTEM, height=220)

    question = st.text_area("Analysevraag", height=120)
    if not question.strip():
        st.stop()

    qvec = embed_query(oai, settings.embedding_model, question)
    rows = match_chunks(supabase, settings.rpc_match_fn, query_embedding=qvec, k=k, cao_id=filter_cao_id)
    if not rows:
        st.info("Geen bronnen gevonden.")
        st.stop()

    sources = build_sources_context(rows)
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

    with st.spinner("Genereren..."):
        resp = oai.chat.completions.create(
            model=settings.chat_model,
            messages=messages,
            temperature=0.2,
        )
        answer = resp.choices[0].message.content or ""

    st.subheader("Antwoord")
    ok = citation_check(answer, n_sources=len(rows))
    if not ok:
        st.warning("Uitvoer voldoet niet aan citation-contract (onvoldoende/ongeldige citations).")
    st.write(answer)

    st.subheader("Bronnen")
    for i, r in enumerate(rows, 1):
        st.markdown(
            f"**[S{i}]** cao_id={r.get('cao_id')} | chunk={r.get('chunk_index')} | "
            f"pages={r.get('page_start')}-{r.get('page_end')} | chunk_id={r.get('chunk_id')}"
        )
        st.write((r.get("chunk_content") or "").strip())
        st.divider()