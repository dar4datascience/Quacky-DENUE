from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class LoginConfig:
    username: str | None = None
    password: str | None = None
    username_selector: str = "input[type='email'], input[name='username']"
    password_selector: str = "input[type='password']"
    submit_selector: str = "button[type='submit'], input[type='submit']"


@dataclass(slots=True)
class PipelineConfig:
    download_url: str
    download_dir: Path
    storage_backend: str
    duckdb_path: Path
    parquet_dir: Path
    report_path: Path
    chunk_size: int = 50_000
    headless: bool = True
    max_files: int | None = None
    federation_filter: set[str] | None = None
    login: LoginConfig | None = None
