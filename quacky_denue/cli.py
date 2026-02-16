from __future__ import annotations

import argparse
import os
from pathlib import Path

from quacky_denue.config import LoginConfig, PipelineConfig
from quacky_denue.logging_utils import configure_logging
from quacky_denue.pipeline import run_pipeline


def _csv_to_set(value: str | None) -> set[str] | None:
    if not value:
        return None
    parsed = {item.strip() for item in value.split(",") if item.strip()}
    return parsed or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quacky DENUE periodic ingestion pipeline")
    parser.add_argument(
        "--download-url",
        default="https://www.inegi.org.mx/app/descarga/default.html#",
        help="INEGI downloads page URL",
    )
    parser.add_argument("--download-dir", default="data/downloads", help="Zip download folder")
    parser.add_argument(
        "--storage-backend",
        choices=("duckdb", "parquet"),
        default="duckdb",
        help="Storage backend (duckdb recommended for append-heavy periodic loads)",
    )
    parser.add_argument("--duckdb-path", default="data/denue_historical.duckdb")
    parser.add_argument("--parquet-dir", default="data/parquet")
    parser.add_argument("--report-path", default="reports/extraction_report.json")
    parser.add_argument("--chunk-size", type=int, default=50000)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument(
        "--federations",
        default=None,
        help="Optional comma-separated federation IDs to process, e.g. 09,15,31-33",
    )
    parser.add_argument("--headless", action="store_true", default=False)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(level=args.log_level)

    login_username = os.getenv("DENUE_LOGIN_USERNAME")
    login_password = os.getenv("DENUE_LOGIN_PASSWORD")
    login_config = None
    if login_username and login_password:
        login_config = LoginConfig(username=login_username, password=login_password)

    config = PipelineConfig(
        download_url=args.download_url,
        download_dir=Path(args.download_dir),
        storage_backend=args.storage_backend,
        duckdb_path=Path(args.duckdb_path),
        parquet_dir=Path(args.parquet_dir),
        report_path=Path(args.report_path),
        chunk_size=args.chunk_size,
        max_files=args.max_files,
        federation_filter=_csv_to_set(args.federations),
        headless=args.headless,
        login=login_config,
    )

    run_pipeline(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
