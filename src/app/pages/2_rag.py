# rag.py
from __future__ import annotations

import os

import streamlit as st

from clients.openai_client import get_openai_client
from clients.supabase_client import get_supabase_client
from core.errors import MissingConfigError
from core.settings import load_settings, require_openai, require_supabase
from services.rag_service import RagService

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


def run_rag_page() -> None:
    try:
        settings = load_settings(st.secrets, os.environ)
        require_supabase(settings.supabase)
        require_openai(settings.openai)
    except MissingConfigError as exc:
        st.error(str(exc))
        st.stop()

    supabase = get_supabase_client(settings.supabase)
    oai = get_openai_client(settings.openai)
    service = RagService(
        supabase,
        oai,
        embedding_model=settings.openai.embedding_model,
        chat_model=settings.openai.chat_model,
        rpc_match_fn=settings.rpc_match_fn,
    )

    st.title("RAG")
    st.caption("Analyse over CAO-teksten met bronvermelding.")

    with st.sidebar:
        st.subheader("Scope")
        docs = service.list_documents()
        options = [("Alle CAO's", None)] + [(f"{d['cao_name']} ({d['cao_id']})", d["cao_id"]) for d in docs]
        label_to_id = {label: cid for label, cid in options}
        selected_label = st.selectbox("CAO", [x[0] for x in options], index=0)
        filter_cao_id = label_to_id[selected_label]

        k = st.slider("Context chunks", min_value=6, max_value=40, value=16, step=1)
        system_rules = st.text_area("System rules", value=DEFAULT_SYSTEM, height=220)

    question = st.text_area("Analysevraag", height=120)
    if not question.strip():
        st.stop()

    qvec = service.embed_query(question)
    rows = service.match_chunks(query_embedding=qvec, k=k, cao_id=filter_cao_id)
    if not rows:
        st.info("Geen bronnen gevonden.")
        st.stop()

    sources = service.build_sources_context(rows)

    with st.spinner("Genereren..."):
        answer = service.generate_answer(question, system_rules, sources)

    st.subheader("Antwoord")
    ok = service.citation_check(answer, n_sources=len(rows))
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


run_rag_page()
