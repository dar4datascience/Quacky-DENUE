from __future__ import annotations

import logging
import re

from quacky_denue.config import PipelineConfig
from quacky_denue.discovery import discover_denue_links, validate_link_count
from quacky_denue.download import download_zip
from quacky_denue.models import FileProcessingStats, PipelineReport
from quacky_denue.reader import infer_snapshot_period, iter_denue_chunks
from quacky_denue.reporting import utcnow, write_report
from quacky_denue.schema import normalize_chunk
from quacky_denue.storage import choose_storage_backend

LOGGER = logging.getLogger(__name__)
TABLE_SAFE = re.compile(r"[^a-z0-9_]+")


def _safe_table_name(snapshot_period: str) -> str:
    cleaned = TABLE_SAFE.sub("_", snapshot_period.lower()).strip("_")
    return f"denue_{cleaned or 'unknown'}"


def run_pipeline(config: PipelineConfig) -> PipelineReport:
    report = PipelineReport(started_at=utcnow())

    links = discover_denue_links(config)
    report.discovered_links = len(links)
    report.selected_links = len(links)
    report.expected_files = len(links)

    if not validate_link_count(config, len(links)):
        warning = "Discovered link count did not match page badge_denue"
        LOGGER.warning(warning)
        report.errors.append(warning)

    storage = choose_storage_backend(config.storage_backend, config.duckdb_path, config.parquet_dir)

    try:
        for link in links:
            file_stats = FileProcessingStats(
                source_file=link.href,
                federation=link.federation,
                snapshot_period="unknown",
            )

            try:
                zip_path = download_zip(link, config.download_dir)
                report.downloaded_files += 1
                file_stats.source_file = str(zip_path)

                snapshot_period = infer_snapshot_period(zip_path)
                file_stats.snapshot_period = snapshot_period
                table_name = _safe_table_name(snapshot_period)

                for chunk in iter_denue_chunks(zip_path, chunk_size=config.chunk_size):
                    normalized, missing_required, unknown_cols = normalize_chunk(
                        chunk,
                        snapshot_period=snapshot_period,
                        source_file=str(zip_path),
                        federation=link.federation,
                    )

                    file_stats.missing_required_columns = sorted(
                        set(file_stats.missing_required_columns + missing_required)
                    )
                    file_stats.unknown_columns = sorted(
                        set(file_stats.unknown_columns + unknown_cols)
                    )

                    file_stats.total_rows += len(normalized)
                    written = storage.write(normalized, table_name)
                    file_stats.written_rows += written

                report.processed_files += 1
            except Exception as exc:  # noqa: BLE001 - report and continue to next file
                message = f"Failed file {link.href}: {exc}"
                LOGGER.exception(message)
                file_stats.errors.append(str(exc))
                report.errors.append(message)

            report.total_rows += file_stats.total_rows
            report.written_rows += file_stats.written_rows
            report.file_reports.append(file_stats)
    finally:
        storage.close()

    report.finished_at = utcnow()
    write_report(report, config.report_path)

    LOGGER.info(
        "Pipeline complete | files=%s/%s rows=%s written=%s completeness=%.2f%%",
        report.processed_files,
        report.expected_files,
        report.total_rows,
        report.written_rows,
        100 * report.completeness_ratio(),
    )

    return report
