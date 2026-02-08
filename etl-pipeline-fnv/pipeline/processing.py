from __future__ import annotations

import logging
import time
from typing import List

from openai import OpenAI
from supabase import Client

from pipeline.config import ProcessingSettings
from pipeline.embeddings import embed_texts
from pipeline.pdf_parsing import chunk_text, extract_text_with_page_map, pages_for_chunk
from pipeline.supabase_io import (
    download_pdf_from_storage,
    iter_documents_to_process,
    mark_processed,
    upsert_chunks,
)

log = logging.getLogger("process_pdfs")


def process_one_document(
    supabase: Client,
    openai_client: OpenAI,
    settings: ProcessingSettings,
    doc: dict,
) -> int:
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
        return 0

    chunk_tuples = chunk_text(full_text, settings.chunk_chars)
    if not chunk_tuples:
        log.warning("No chunks produced for cao_id=%s", cao_id)
        mark_processed(supabase, cao_id)
        return 0

    rows: List[dict] = []

    for base in range(0, len(chunk_tuples), settings.embed_batch):
        batch = chunk_tuples[base:base + settings.embed_batch]
        texts = [t[2] for t in batch]
        vectors = embed_texts(openai_client, settings.embed_model, texts)

        for idx, ((char_start, char_end, content), vec) in enumerate(zip(batch, vectors)):
            chunk_index = base + idx
            chunk_id = f"{cao_id}::{chunk_index}"
            page_start, page_end = pages_for_chunk(page_spans, char_start, char_end)

            rows.append(
                {
                    "chunk_id": chunk_id,
                    "cao_id": cao_id,
                    "chunk_index": chunk_index,
                    "chunk_content": content,
                    "embedding": vec,
                    "page_start": page_start,
                    "page_end": page_end,
                    "char_start": char_start,
                    "char_end": char_end,
                }
            )

        if settings.sleep_s > 0:
            time.sleep(settings.sleep_s)

        if len(rows) >= settings.upsert_batch * 3:
            upsert_chunks(supabase, rows, settings.upsert_batch)
            rows.clear()

    if rows:
        upsert_chunks(supabase, rows, settings.upsert_batch)

    mark_processed(supabase, cao_id)
    log.info("Done cao_id=%s (%d chunks).", cao_id, len(chunk_tuples))
    return len(chunk_tuples)


def process_documents(
    supabase: Client,
    openai_client: OpenAI,
    settings: ProcessingSettings,
    *,
    only_unprocessed: bool,
    limit: int,
) -> int:
    docs = iter_documents_to_process(supabase, only_unprocessed=only_unprocessed, limit=limit)
    if not docs:
        log.info("No documents to process.")
        return 0

    total = 0
    for d in docs:
        total += process_one_document(supabase, openai_client, settings, d)
    return total
