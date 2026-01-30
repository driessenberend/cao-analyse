# documents.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import streamlit as st
from supabase import create_client, Client


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_anon_key: str

    @staticmethod
    def load() -> "Settings":
        supa = st.secrets.get("supabase", {})
        return Settings(
            supabase_url=supa.get("url") or os.environ["SUPABASE_URL"],
            supabase_anon_key=supa.get("anon_key") or os.environ["SUPABASE_ANON_KEY"],
        )


@st.cache_resource
def get_supabase(settings: Settings) -> Client:
    return create_client(settings.supabase_url, settings.supabase_anon_key)


def fetch_documents(supabase: Client, limit: int = 2000) -> List[Dict[str, Any]]:
    res = (
        supabase.table("cao_documents")
        .select("cao_id,cao_name,source_url,storage_bucket,storage_path,file_sha256,file_bytes,processed_at,ingested_at")
        .order("cao_name")
        .limit(limit)
        .execute()
    )
    return res.data or []


def fetch_chunk_count(supabase: Client, cao_id: str) -> int:
    res = (
        supabase.table("cao_chunks")
        .select("chunk_id", count="exact")
        .eq("cao_id", cao_id)
        .limit(1)
        .execute()
    )
    return int(res.count or 0)


def try_get_pdf_url(supabase: Client, bucket: str, path: str) -> Optional[str]:
    storage = supabase.storage.from_(bucket)

    try:
        pub = storage.get_public_url(path)
        if isinstance(pub, dict):
            url = pub.get("publicUrl") or pub.get("data", {}).get("publicUrl")
            if url:
                return url
        if isinstance(pub, str):
            return pub
    except Exception:
        pass

    try:
        signed = storage.create_signed_url(path, 60 * 60)
        if isinstance(signed, dict):
            url = signed.get("signedUrl") or signed.get("data", {}).get("signedUrl")
            if url:
                return url
        if isinstance(signed, str):
            return signed
    except Exception:
        pass

    return None


def run_documents_page() -> None:
    settings = Settings.load()
    supabase = get_supabase(settings)

    st.title("Documents")
    st.caption("Overzicht van CAO-PDF’s in Supabase + verwerkingsstatus + chunks.")

    docs = fetch_documents(supabase)
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
        url = try_get_pdf_url(supabase, bucket, path)
        if url:
            st.link_button("Open PDF", url)
        else:
            st.info("Geen (public/signed) URL beschikbaar met huidige credentials/policies.")

    st.subheader("Chunks")
    cnt = fetch_chunk_count(supabase, cao_id)
    st.write({"chunk_count": cnt})

    preview_n = st.slider("Preview chunks", min_value=3, max_value=30, value=10, step=1)
    rows = (
        supabase.table("cao_chunks")
        .select("chunk_id,chunk_index,page_start,page_end,chunk_content")
        .eq("cao_id", cao_id)
        .order("chunk_index")
        .limit(preview_n)
        .execute()
        .data
        or []
    )

    for r in rows:
        st.markdown(
            f"**chunk={r.get('chunk_index')} | pages={r.get('page_start')}-{r.get('page_end')} | chunk_id={r.get('chunk_id')}**"
        )
        st.write((r.get("chunk_content") or "").strip())
        st.divider()