from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from core.errors import MissingConfigError


@dataclass(frozen=True)
class SupabaseSettings:
    url: str
    anon_key: str


@dataclass(frozen=True)
class OpenAISettings:
    api_key: str
    embedding_model: str
    chat_model: str


@dataclass(frozen=True)
class AppSettings:
    supabase: SupabaseSettings
    openai: OpenAISettings
    rpc_match_fn: str


@dataclass(frozen=True)
class EtlSettings:
    supabase_url: str
    supabase_service_role_key: str
    openai_api_key: str


def load_settings(
    secrets: Mapping[str, Any],
    env: Mapping[str, str],
    *,
    embedding_model: str = "text-embedding-3-small",
    chat_model: str = "gpt-4.1-mini",
    rpc_match_fn: str = "match_cao_chunks",
) -> AppSettings:
    supa = secrets.get("supabase", {})
    oai = secrets.get("openai", {})

    supabase = SupabaseSettings(
        url=supa.get("url") or env.get("SUPABASE_URL", ""),
        anon_key=supa.get("anon_key") or env.get("SUPABASE_ANON_KEY", ""),
    )
    openai = OpenAISettings(
        api_key=oai.get("api_key") or env.get("OPENAI_API_KEY", ""),
        embedding_model=embedding_model,
        chat_model=chat_model,
    )
    return AppSettings(supabase=supabase, openai=openai, rpc_match_fn=rpc_match_fn)


def require_supabase(settings: SupabaseSettings) -> None:
    if not settings.url or not settings.anon_key:
        raise MissingConfigError(
            "Supabase credentials ontbreken. Voeg ze toe aan Streamlit secrets (supabase.url / supabase.anon_key)."
        )


def require_openai(settings: OpenAISettings) -> None:
    if not settings.api_key:
        raise MissingConfigError("OpenAI API key ontbreekt. Voeg openai.api_key toe aan Streamlit secrets.")


def load_etl_settings(
    secrets: Mapping[str, Any],
    env: Mapping[str, str],
) -> EtlSettings:
    supa = secrets.get("supabase", {})
    oai = secrets.get("openai", {})
    return EtlSettings(
        supabase_url=supa.get("url") or env.get("SUPABASE_URL", ""),
        supabase_service_role_key=supa.get("service_role_key") or env.get("SUPABASE_SERVICE_ROLE_KEY", ""),
        openai_api_key=oai.get("api_key") or env.get("OPENAI_API_KEY", ""),
    )


def require_etl_settings(settings: EtlSettings) -> None:
    missing = []
    if not settings.supabase_url:
        missing.append("SUPABASE_URL / supabase.url")
    if not settings.supabase_service_role_key:
        missing.append("SUPABASE_SERVICE_ROLE_KEY / supabase.service_role_key")
    if not settings.openai_api_key:
        missing.append("OPENAI_API_KEY / openai.api_key")
    if missing:
        joined = ", ".join(missing)
        raise MissingConfigError(f"ETL secrets ontbreken: {joined}.")
