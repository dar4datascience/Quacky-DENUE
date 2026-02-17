# Quacky-DENUE

Python-first DENUE ingestion pipeline designed for **periodic extraction**, **schema homogenization**, and **memory-conscious storage**.

## What this revamp includes

- Safe retry patterns for fragile operations (downloads + optional login flow)
- Structured extraction by federation from INEGI download links
- Schema validation and schema resolution to normalize shifting source schemas
- Detailed extraction reporting (errors, processed files, row counts, completeness ratio)
- Pluggable storage backend with `duckdb` (default) or `parquet`

## Repository structure

- `quacky_denue/cli.py`: CLI entrypoint and runtime configuration
- `quacky_denue/pipeline.py`: orchestration layer
- `quacky_denue/discovery.py`: link discovery + optional login + validation
- `quacky_denue/download.py`: resilient zip download logic
- `quacky_denue/reader.py`: zip/csv reader with chunked loading
- `quacky_denue/schema.py`: schema normalization and validation
- `quacky_denue/storage.py`: DuckDB/Parquet writing backends
- `quacky_denue/reporting.py`: extraction report output

## Install

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Run

```bash
python -m quacky_denue \
  --storage-backend duckdb \
  --download-dir data/downloads \
  --duckdb-path data/denue_historical.duckdb \
  --report-path reports/extraction_report.json \
  --headless
```

Optional filtering by federations:

```bash
python -m quacky_denue --federations 09,15,31-33 --headless
```

## Full historical ETL runner (all discovered links)

Use the dedicated script when you want a run-scoped artifact folder with durable logs/reports for future retries:

```bash
./.venv/bin/python scripts/run_full_etl.py \
  --storage-backend duckdb \
  --duckdb-path data/denue_historical.duckdb \
  --output-root runs
```

What it writes per run:

- `runs/<run_id>/pipeline.log` (detailed progress log)
- `runs/<run_id>/extraction_report.json` (pipeline report)
- `runs/<run_id>/failed_files.json` (file-level failures, useful for retry planning)
- `runs/run_history.jsonl` (append-only run summary history)

For targeted retry runs, you can scope a run by federations:

```bash
./.venv/bin/python scripts/run_full_etl.py --federations 09,15
```

Generate a retry command directly from a failed run artifact:

```bash
./.venv/bin/python scripts/build_retry_command.py \
  --failed-files runs/<run_id>/failed_files.json
```

This prints a ready-to-run `scripts/run_full_etl.py` command, prefilled with:

- inferred failed federations
- `--max-files` defaulted to the failed entry count
- the same storage/output defaults (customizable via flags)

## Optional login (safe retry)

If the portal ever requires auth, set credentials via environment variables:

```bash
export DENUE_LOGIN_USERNAME="your_user"
export DENUE_LOGIN_PASSWORD="your_password"
```

The pipeline retries login safely up to 3 times with exponential backoff.

## Storage backend decision (Parquet vs DuckDB)

### Recommended default: DuckDB

Use `--storage-backend duckdb` when running periodic append workloads:

- Better append semantics and SQL analytics
- Avoids full-file rewrites on incremental loads
- Good memory behavior when ingesting chunked data

### When to use Parquet

Use `--storage-backend parquet` for open file interoperability. This implementation writes append-only parquet chunk files per snapshot table.

## Reporting and completeness

Each run writes a JSON report (default: `reports/extraction_report.json`) including:

- discovered/selected/downloaded/processed file counts
- total rows seen and rows written
- per-file missing required columns
- per-file unknown columns
- per-file errors
- overall `completeness_ratio`

## Periodic pipeline usage

For scheduled runs (cron, systemd timer, or orchestration tool):

1. activate environment
2. execute `python -m quacky_denue --headless`
3. archive the generated report for observability and audits

## Testing

Run the default suite (fast, no live network):

```bash
python -m pytest tests -m "not live" -v
```

Run live scraping smoke test against INEGI page (network required):

```bash
export RUN_LIVE_DENUE_TESTS=1
python -m pytest tests/test_live_scraping.py -m live -v
```

Live tests verify:
- links are scraped from `https://www.inegi.org.mx/app/descarga/?ti=6`
- discovered URLs match valid DENUE zip patterns
- federation IDs are parsed correctly for downstream filtering
- multi-year integration for one federation works with live downloads and DuckDB writes