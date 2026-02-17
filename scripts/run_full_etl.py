from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from quacky_denue.config import PipelineConfig
from quacky_denue.pipeline import run_pipeline

LOGGER = logging.getLogger("quacky_denue.full_etl")


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _csv_to_set(value: str | None) -> set[str] | None:
    if not value:
        return None
    parsed = {item.strip() for item in value.split(",") if item.strip()}
    return parsed or None


def _configure_run_logging(log_path: Path, level: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


def _append_run_history(history_path: Path, payload: dict) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full DENUE ETL for all discovered CSV links")
    parser.add_argument(
        "--download-url",
        default="https://www.inegi.org.mx/app/descarga/?ti=6",
        help="INEGI downloads page URL",
    )
    parser.add_argument("--output-root", default="runs", help="Root folder for run artifacts")
    parser.add_argument("--storage-backend", choices=("duckdb", "parquet"), default="duckdb")
    parser.add_argument("--duckdb-path", default="data/denue_historical.duckdb")
    parser.add_argument("--parquet-dir", default="data/parquet")
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument(
        "--federations",
        default=None,
        help="Optional comma-separated federation IDs for partial/retry runs, e.g. 09,15,31-33",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run browser in headed mode (default is headless)",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = _utc_timestamp()
    run_dir = Path(args.output_root) / run_id
    report_path = run_dir / "extraction_report.json"
    log_path = run_dir / "pipeline.log"
    history_path = Path(args.output_root) / "run_history.jsonl"

    _configure_run_logging(log_path, args.log_level)

    config = PipelineConfig(
        download_url=args.download_url,
        download_dir=run_dir / "downloads",
        storage_backend=args.storage_backend,
        duckdb_path=Path(args.duckdb_path),
        parquet_dir=Path(args.parquet_dir),
        report_path=report_path,
        chunk_size=args.chunk_size,
        max_files=args.max_files,
        federation_filter=_csv_to_set(args.federations),
        headless=not args.headed,
        login=None,
    )

    LOGGER.info("Starting full DENUE ETL | run_id=%s", run_id)

    try:
        report = run_pipeline(config)
        failed_files = [
            {
                "source_file": item.source_file,
                "federation": item.federation,
                "snapshot_period": item.snapshot_period,
                "errors": item.errors,
            }
            for item in report.file_reports
            if item.errors
        ]

        failed_path = run_dir / "failed_files.json"
        failed_path.write_text(json.dumps(failed_files, ensure_ascii=False, indent=2), encoding="utf-8")

        summary = {
            "run_id": run_id,
            "finished_at": report.finished_at.isoformat() if report.finished_at else None,
            "status": "success" if not report.errors else "completed_with_errors",
            "report_path": str(report_path),
            "log_path": str(log_path),
            "failed_files_path": str(failed_path),
            "discovered_links": report.discovered_links,
            "selected_links": report.selected_links,
            "downloaded_files": report.downloaded_files,
            "processed_files": report.processed_files,
            "expected_files": report.expected_files,
            "total_rows": report.total_rows,
            "written_rows": report.written_rows,
            "errors": report.errors,
        }
        _append_run_history(history_path, summary)

        LOGGER.info(
            "ETL finished | run_id=%s processed=%s/%s rows=%s written=%s failed_files=%s",
            run_id,
            report.processed_files,
            report.expected_files,
            report.total_rows,
            report.written_rows,
            len(failed_files),
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        summary = {
            "run_id": run_id,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed",
            "report_path": str(report_path),
            "log_path": str(log_path),
            "error": str(exc),
        }
        _append_run_history(history_path, summary)
        LOGGER.exception("ETL failed | run_id=%s", run_id)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
