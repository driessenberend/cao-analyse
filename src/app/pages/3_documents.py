# documents.py
from __future__ import annotations

import os

import streamlit as st

from clients.supabase_client import get_supabase_client
from core.errors import MissingConfigError
from core.settings import load_etl_settings, load_settings, require_etl_settings, require_supabase
from services.documents_service import DocumentsService
from services.etl_service import EtlService

def run_documents_page() -> None:
    try:
        settings = load_settings(st.secrets, os.environ)
        require_supabase(settings.supabase)
    except MissingConfigError as exc:
        st.error(str(exc))
        st.stop()

    supabase = get_supabase_client(settings.supabase)
    service = DocumentsService(supabase)

    st.title("Documents")
    st.caption("Overzicht van CAO-PDF’s in Supabase + verwerkingsstatus + chunks.")

    docs = service.list_documents()
    if not docs:
        st.info("Geen documenten gevonden.")
        st.stop()

    q = st.text_input("Filter op naam of cao_id", value="")
    if q:
        ql = q.lower().strip()
        docs = [d for d in docs if ql in (d.get("cao_name") or "").lower() or ql in (d.get("cao_id") or "").lower()]

    selected = st.selectbox(
        "Selecteer CAO",
        options=docs,
        format_func=lambda d: f"{d.get('cao_name')} ({d.get('cao_id')})",
    )
    if not selected:
        st.stop()

    cao_id = selected["cao_id"]

    st.subheader("Metadata")
    st.write(
        {
            "cao_id": cao_id,
            "cao_name": selected.get("cao_name"),
            "source_url": selected.get("source_url"),
            "storage_bucket": selected.get("storage_bucket"),
            "storage_path": selected.get("storage_path"),
            "file_bytes": selected.get("file_bytes"),
            "file_sha256": selected.get("file_sha256"),
            "ingested_at": selected.get("ingested_at"),
            "processed_at": selected.get("processed_at"),
        }
    )

    st.subheader("PDF")
    bucket = selected.get("storage_bucket")
    path = selected.get("storage_path")
    if bucket and path:
        url = service.get_pdf_url(bucket, path)
        if url:
            st.link_button("Open PDF", url)
        else:
            st.info("Geen (public/signed) URL beschikbaar met huidige credentials/policies.")

    st.subheader("Chunks")
    cnt = service.chunk_count(cao_id)
    st.write({"chunk_count": cnt})

    preview_n = st.slider("Preview chunks", min_value=3, max_value=30, value=10, step=1)
    rows = service.chunk_preview(cao_id, preview_n)

    for r in rows:
        st.markdown(
            f"**chunk={r.get('chunk_index')} | pages={r.get('page_start')}-{r.get('page_end')} | chunk_id={r.get('chunk_id')}**"
        )
        st.write((r.get("chunk_content") or "").strip())
        st.divider()

    st.subheader("ETL pipeline")
    st.caption("Run scraping, ingest, en processing vanuit de UI (vereist service role key).")
    etl_settings = load_etl_settings(st.secrets, os.environ)
    try:
        require_etl_settings(etl_settings)
    except MissingConfigError as exc:
        st.info(str(exc))
        st.stop()

    etl_service = EtlService(EtlService.repo_root_from_here())
    script_paths = {
        "Scrape FNV PDFs": "etl-pipeline-fnv/scripts/scraping.py",
        "Ingest naar Supabase": "etl-pipeline-fnv/scripts/ingest_to_supabase.py",
        "Process (chunks + embeddings)": "etl-pipeline-fnv/scripts/main.py",
    }

    if "etl_output" not in st.session_state:
        st.session_state["etl_output"] = ""

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Run scraping"):
            with st.spinner("Scraping..."):
                res = etl_service.run_script(script_paths["Scrape FNV PDFs"], [], settings=etl_settings)
            st.session_state["etl_output"] = res.output or f"Exit code: {res.returncode}"
    with col2:
        if st.button("Run ingest"):
            with st.spinner("Ingest..."):
                res = etl_service.run_script(script_paths["Ingest naar Supabase"], [], settings=etl_settings)
            st.session_state["etl_output"] = res.output or f"Exit code: {res.returncode}"
    with col3:
        if st.button("Run processing"):
            with st.spinner("Processing..."):
                res = etl_service.run_script(script_paths["Process (chunks + embeddings)"], ["--only-unprocessed"], settings=etl_settings)
            st.session_state["etl_output"] = res.output or f"Exit code: {res.returncode}"

    st.text_area("ETL output", st.session_state["etl_output"], height=200)


run_documents_page()
