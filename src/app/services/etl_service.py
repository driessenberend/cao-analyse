from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from core.settings import EtlSettings


@dataclass(frozen=True)
class EtlResult:
    returncode: int
    output: str


class EtlService:
    def __init__(self, repo_root: Path) -> None:
        self._repo_root = repo_root

    @staticmethod
    def repo_root_from_here() -> Path:
        return Path(__file__).resolve().parents[3]

    def run_script(
        self,
        script_path: str,
        args: Iterable[str],
        *,
        settings: EtlSettings,
    ) -> EtlResult:
        env = os.environ.copy()
        env["SUPABASE_URL"] = settings.supabase_url
        env["SUPABASE_SERVICE_ROLE_KEY"] = settings.supabase_service_role_key
        env["OPENAI_API_KEY"] = settings.openai_api_key

        cmd = [sys.executable, script_path, *args]
        proc = subprocess.run(
            cmd,
            cwd=self._repo_root,
            env=env,
            capture_output=True,
            text=True,
        )
        output = (proc.stdout or "") + (proc.stderr or "")
        return EtlResult(returncode=proc.returncode, output=output.strip())
