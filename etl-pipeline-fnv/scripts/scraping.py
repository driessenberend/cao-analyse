from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys  # noqa: E402

sys.path.append(str(ROOT))

from pipeline.config import ScrapeSettings  # noqa: E402
from pipeline.scraper import run_scrape  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape CAO PDFs + manifest from FNV.")
    parser.add_argument("--base-url", default="https://www.fnv.nl")
    parser.add_argument("--start-url", default="https://www.fnv.nl/cao-sector")
    parser.add_argument("--out-dir", default="data-raw")
    parser.add_argument("--manifest", default="data-raw/manifest.jsonl")
    args = parser.parse_args()

    settings = ScrapeSettings(
        base_url=args.base_url,
        start_url=args.start_url,
        out_dir=Path(args.out_dir),
        manifest_path=Path(args.manifest),
    )
    run_scrape(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
