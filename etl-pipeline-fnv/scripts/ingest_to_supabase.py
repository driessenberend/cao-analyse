from __future__ import annotations

import argparse
import os
from pathlib import Path

from supabase import create_client

ROOT = Path(__file__).resolve().parents[1]
import sys  # noqa: E402

sys.path.append(str(ROOT))

from pipeline.config import IngestSettings, SupabaseSettings  # noqa: E402
from pipeline.ingest import ingest_documents  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload scraped CAO PDFs + metadata into Supabase.")
    parser.add_argument("--manifest", default="data-raw/manifest.jsonl")
    parser.add_argument("--data-dir", default="data-raw")
    parser.add_argument("--bucket", default="cao-pdfs")
    args = parser.parse_args()

    supabase_settings = SupabaseSettings(
        url=os.environ["SUPABASE_URL"],
        service_role_key=os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
    ingest_settings = IngestSettings(
        storage_bucket=args.bucket,
        manifest_path=Path(args.manifest),
        data_dir=Path(args.data_dir),
    )

    supabase = create_client(supabase_settings.url, supabase_settings.service_role_key)
    ingest_documents(supabase, ingest_settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
