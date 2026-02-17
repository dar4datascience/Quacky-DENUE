from __future__ import annotations

import os
import re
from zipfile import ZipFile
from unittest.mock import patch

import duckdb

import pytest

from quacky_denue.config import PipelineConfig
from quacky_denue.discovery import discover_denue_links, is_denue_csv_zip_url
from quacky_denue.download import download_zip
from quacky_denue.pipeline import run_pipeline
from quacky_denue.reader import iter_denue_chunks


@pytest.mark.live
@pytest.mark.skipif(
    os.getenv("RUN_LIVE_DENUE_TESTS") != "1",
    reason="Set RUN_LIVE_DENUE_TESTS=1 to run live INEGI scraping smoke tests",
)
def test_live_discovery_inegi_denue_links(tmp_path):
    config = PipelineConfig(
        download_url="https://www.inegi.org.mx/app/descarga/?ti=6",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "live_test.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report.json",
        headless=True,
        max_files=40,
    )

    links = discover_denue_links(config)

    assert len(links) > 0
    assert all(is_denue_csv_zip_url(link.href) for link in links)
    assert all(re.fullmatch(r"\d{2}(?:-\d{2})?", link.federation) for link in links)


def _extract_year_from_url(url: str) -> str | None:
    match = re.search(r"denue_[0-9]{2}(?:-[0-9]{2})?_([0-9]{4})[0-9]{4}_csv\.zip$", url)
    if match:
        year = match.group(1)
        # Guard against malformed dates like 2502
        return year if year.isdigit() and 2000 <= int(year) <= 2030 else None
    return None


def _frame_contains_replacement_char(df) -> bool:
    for col in df.select_dtypes(include=["object", "string"]).columns:
        series = df[col].dropna().astype(str)
        if series.str.contains("\ufffd", regex=False).any():
            return True
    return False


@pytest.mark.live
@pytest.mark.skipif(
    os.getenv("RUN_LIVE_DENUE_TESTS") != "1",
    reason="Set RUN_LIVE_DENUE_TESTS=1 to run live INEGI scraping smoke tests",
)
def test_live_zip_structure_and_conjunto_csv_readability(tmp_path):
    config = PipelineConfig(
        download_url="https://www.inegi.org.mx/app/descarga/?ti=6",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "zip_structure.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "zip_structure_report.json",
        headless=True,
        federation_filter={"09"},
        max_files=20,
    )

    links = discover_denue_links(config)
    assert links, "No live links discovered for federation 09"

    zip_path = download_zip(links[0], config.download_dir)
    assert zip_path.exists()

    with ZipFile(zip_path) as archive:
        names_lower = [name.lower() for name in archive.namelist()]
        assert any("conjunto" in name and "datos" in name and name.endswith(".csv") for name in names_lower)
        assert any("diccionario" in name and name.endswith(".csv") for name in names_lower)
        assert any("metadatos" in name and name.endswith(".txt") for name in names_lower)

    first_chunk = next(iter_denue_chunks(zip_path, chunk_size=5_000))
    assert len(first_chunk) > 0
    assert not _frame_contains_replacement_char(first_chunk)


@pytest.mark.live
@pytest.mark.skipif(
    os.getenv("RUN_LIVE_DENUE_TESTS") != "1",
    reason="Set RUN_LIVE_DENUE_TESTS=1 to run live INEGI scraping smoke tests",
)
def test_live_pipeline_multi_year_estado_integration(tmp_path):
    # Discover live links first
    discovery_config = PipelineConfig(
        download_url="https://www.inegi.org.mx/app/descarga/?ti=6",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "live_multi_year.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report.json",
        headless=True,
        federation_filter={"09"},
        max_files=250,
    )
    links = discover_denue_links(discovery_config)

    # Pick one link per distinct year and keep two years to bound test runtime
    by_year = {}
    for link in links:
        year = _extract_year_from_url(link.href)
        if year and year not in by_year:
            by_year[year] = link

    if len(by_year) < 1:
        pytest.skip("No live federation snapshots with identifiable years for integration test")

    selected_years = sorted(by_year.keys())[:2]
    selected_links = [by_year[year] for year in selected_years]

    pipeline_config = PipelineConfig(
        download_url="https://www.inegi.org.mx/app/descarga/?ti=6",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "live_multi_year.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "multi_year_report.json",
        headless=True,
        chunk_size=200_000,
    )

    # Reuse live links, but bypass rediscovery variability for deterministic processing.
    with patch("quacky_denue.pipeline.discover_denue_links", return_value=selected_links), patch(
        "quacky_denue.pipeline.validate_link_count", return_value=True
    ):
        report = run_pipeline(pipeline_config)

    assert report.processed_files == len(selected_links)
    assert report.written_rows > 0
    assert report.completeness_ratio() == 1.0

    conn = duckdb.connect(str(pipeline_config.duckdb_path))
    try:
        total_rows = 0
        for year in selected_years:
            table_name = f"denue_{year}"
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert row_count > 0
            total_rows += row_count
        assert total_rows > 0
    finally:
        conn.close()
