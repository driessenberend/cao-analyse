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
from pathlib import Path

from openai import OpenAI
from supabase import create_client

ROOT = Path(__file__).resolve().parents[1]
import sys  # noqa: E402

sys.path.append(str(ROOT))

from pipeline.config import OpenAISettings, ProcessingSettings, SupabaseSettings  # noqa: E402
from pipeline.processing import process_documents  # noqa: E402


# ----------------------------- logging -----------------------------

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


log = logging.getLogger("process_pdfs")


# ----------------------------- config -----------------------------

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

    supabase_settings = SupabaseSettings(
        url=os.environ["SUPABASE_URL"],
        service_role_key=os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
    openai_settings = OpenAISettings(api_key=os.environ["OPENAI_API_KEY"])
    processing_settings = ProcessingSettings(
        embed_model=args.embed_model,
        embed_dim=args.embed_dim,
        chunk_chars=args.chunk_chars,
        embed_batch=args.embed_batch,
        upsert_batch=args.upsert_batch,
        sleep_s=args.sleep_s,
    )

    supabase = create_client(supabase_settings.url, supabase_settings.service_role_key)
    openai_client = OpenAI(api_key=openai_settings.api_key)

    process_documents(
        supabase,
        openai_client,
        processing_settings,
        only_unprocessed=args.only_unprocessed,
        limit=args.limit,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
