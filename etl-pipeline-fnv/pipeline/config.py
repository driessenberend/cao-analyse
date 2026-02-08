from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SupabaseSettings:
    url: str
    service_role_key: str


@dataclass(frozen=True)
class OpenAISettings:
    api_key: str


@dataclass(frozen=True)
class ProcessingSettings:
    embed_model: str
    embed_dim: int
    chunk_chars: int
    embed_batch: int
    upsert_batch: int
    sleep_s: float


@dataclass(frozen=True)
class IngestSettings:
    storage_bucket: str
    manifest_path: Path
    data_dir: Path


@dataclass(frozen=True)
class ScrapeSettings:
    base_url: str
    start_url: str
    out_dir: Path
    manifest_path: Path
