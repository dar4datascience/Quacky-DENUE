from __future__ import annotations

import argparse
import json
import re
import shlex
from pathlib import Path

FEDERATION_PATTERN = re.compile(r"^\d{2}(?:-\d{2})?$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a retry command for scripts/run_full_etl.py from a failed_files.json artifact"
    )
    parser.add_argument("--failed-files", required=True, help="Path to runs/<run_id>/failed_files.json")
    parser.add_argument("--python-bin", default="./.venv/bin/python", help="Python executable to use")
    parser.add_argument("--download-url", default="https://www.inegi.org.mx/app/descarga/?ti=6")
    parser.add_argument("--storage-backend", choices=("duckdb", "parquet"), default="duckdb")
    parser.add_argument("--duckdb-path", default="data/denue_historical.duckdb")
    parser.add_argument("--parquet-dir", default="data/parquet")
    parser.add_argument("--output-root", default="runs")
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Build command in headed browser mode (default uses headless)",
    )
    return parser.parse_args()


def _load_failed_entries(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("failed_files payload must be a JSON list")
    return [item for item in payload if isinstance(item, dict)]


def _extract_federations(entries: list[dict]) -> list[str]:
    values = {
        str(item.get("federation", "")).strip()
        for item in entries
        if item.get("federation") is not None
    }
    valid = sorted(value for value in values if FEDERATION_PATTERN.fullmatch(value))
    return valid


def _build_command(args: argparse.Namespace, federations: list[str], failed_count: int) -> str:
    cmd: list[str] = [
        args.python_bin,
        "scripts/run_full_etl.py",
        "--download-url",
        args.download_url,
        "--storage-backend",
        args.storage_backend,
        "--duckdb-path",
        args.duckdb_path,
        "--parquet-dir",
        args.parquet_dir,
        "--output-root",
        args.output_root,
        "--chunk-size",
        str(args.chunk_size),
    ]

    if args.headed:
        cmd.append("--headed")

    if federations:
        cmd.extend(["--federations", ",".join(federations)])

    max_files = args.max_files if args.max_files is not None else failed_count
    if max_files > 0:
        cmd.extend(["--max-files", str(max_files)])

    return shlex.join(cmd)


def main() -> int:
    args = parse_args()
    failed_path = Path(args.failed_files)
    if not failed_path.exists():
        raise SystemExit(f"failed files path not found: {failed_path}")

    entries = _load_failed_entries(failed_path)
    federations = _extract_federations(entries)
    command = _build_command(args, federations=federations, failed_count=len(entries))

    print(f"failed_entries={len(entries)}")
    print(f"federations={','.join(federations) if federations else 'none'}")
    print("retry_command:")
    print(command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
