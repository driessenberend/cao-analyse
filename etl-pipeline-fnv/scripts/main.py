"""
Process PDFs stored in Supabase Storage into chunks + embeddings.

Reads from:
- `cao_documents` table (storage_bucket, storage_path, cao_id, cao_name)

Writes to:
- `cao_chunks` (chunk_id, cao_id, chunk_index, chunk_content, embedding, offsets)
- updates `cao_documents.processed_at`

Design choices:
- Idempotent: chunk_id = f"{cao_id}::{chunk_index}" (stable per chunking scheme)
- Batch embeddings: reduce API calls
- Batch upserts: avoid payload limits

Note:
- Chunking by characters is implemented (as requested), but for CAOs better is article-based chunking later.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import fitz  # PyMuPDF
from openai import OpenAI
from supabase import create_client, Client


# ----------------------------- logging -----------------------------

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


log = logging.getLogger("process_pdfs")


# ----------------------------- config -----------------------------

@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_service_role_key: str
    openai_api_key: str
    embed_model: str
    embed_dim: int
    chunk_chars: int
    embed_batch: int
    upsert_batch: int
    sleep_s: float

    @staticmethod
    def from_env(
        *,
        embed_model: str,
        embed_dim: int,
        chunk_chars: int,
        embed_batch: int,
        upsert_batch: int,
        sleep_s: float,
    ) -> "Settings":
        return Settings(
            supabase_url=os.environ["SUPABASE_URL"],
            supabase_service_role_key=os.environ["SUPABASE_SERVICE_ROLE_KEY"],
            openai_api_key=os.environ["OPENAI_API_KEY"],
            embed_model=embed_model,
            embed_dim=embed_dim,
            chunk_chars=chunk_chars,
            embed_batch=embed_batch,
            upsert_batch=upsert_batch,
            sleep_s=sleep_s,
        )

# ----------------------------- main pipeline -----------------------------

def process_one_document(
    supabase: Client,
    openai_client: OpenAI,
    settings: Settings,
    doc: dict,
) -> None:
    cao_id = doc["cao_id"]
    cao_name = doc.get("cao_name")
    bucket = doc["storage_bucket"]
    storage_path = doc["storage_path"]

    log.info("Processing cao_id=%s (%s) from %s/%s", cao_id, cao_name, bucket, storage_path)

    pdf_bytes = download_pdf_from_storage(supabase, bucket, storage_path)

    full_text, page_spans = extract_text_with_page_map(pdf_bytes)
    if not full_text.strip():
        log.warning("Empty extracted text for cao_id=%s", cao_id)
        mark_processed(supabase, cao_id)
        return

    chunk_tuples = chunk_text(full_text, settings.chunk_chars)
    if not chunk_tuples:
        log.warning("No chunks produced for cao_id=%s", cao_id)
        mark_processed(supabase, cao_id)
        return

    rows: List[dict] = []

    # Embed and prepare upsert rows in batches
    for base in range(0, len(chunk_tuples), settings.embed_batch):
        batch = chunk_tuples[base:base + settings.embed_batch]
        texts = [t[2] for t in batch]
        vectors = embed_texts(openai_client, settings.embed_model, texts)

        for idx, ((char_start, char_end, content), vec) in enumerate(zip(batch, vectors)):
            chunk_index = base + idx
            chunk_id = f"{cao_id}::{chunk_index}"
            page_start, page_end = pages_for_chunk(page_spans, char_start, char_end)

            rows.append({
                "chunk_id": chunk_id,
                "cao_id": cao_id,
                "chunk_index": chunk_index,
                "chunk_content": content,
                "embedding": vec,
                "page_start": page_start,
                "page_end": page_end,
                "char_start": char_start,
                "char_end": char_end,
            })

        # throttle to be polite to API / rate limits
        if settings.sleep_s > 0:
            time.sleep(settings.sleep_s)

        # flush periodically to keep memory stable
        if len(rows) >= settings.upsert_batch * 3:
            upsert_chunks(supabase, rows, settings.upsert_batch)
            rows.clear()

    if rows:
        upsert_chunks(supabase, rows, settings.upsert_batch)

    mark_processed(supabase, cao_id)
    log.info("Done cao_id=%s (%d chunks).", cao_id, len(chunk_tuples))


def main() -> int:
    parser = argparse.ArgumentParser(description="Process Supabase-stored PDFs into chunks + OpenAI embeddings.")
    parser.add_argument("--only-unprocessed", action="store_true", help="Only process documents with processed_at is NULL")
    parser.add_argument("--limit", type=int, default=10, help="Max number of documents to process per run")
    parser.add_argument("--chunk-chars", type=int, default=500)
    parser.add_argument("--embed-model", default="text-embedding-3-small")
    parser.add_argument("--embed-dim", type=int, default=1536)
    parser.add_argument("--embed-batch", type=int, default=128)
    parser.add_argument("--upsert-batch", type=int, default=200)
    parser.add_argument("--sleep-s", type=float, default=0.2)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)

    settings = Settings.from_env(
        embed_model=args.embed_model,
        embed_dim=args.embed_dim,
        chunk_chars=args.chunk_chars,
        embed_batch=args.embed_batch,
        upsert_batch=args.upsert_batch,
        sleep_s=args.sleep_s,
    )

    supabase = create_client(settings.supabase_url, settings.supabase_service_role_key)
    openai_client = OpenAI(api_key=settings.openai_api_key)

    docs = iter_documents_to_process(supabase, only_unprocessed=args.only_unprocessed, limit=args.limit)
    if not docs:
        log.info("No documents to process.")
        return 0

    for d in docs:
        process_one_document(supabase, openai_client, settings, d)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())