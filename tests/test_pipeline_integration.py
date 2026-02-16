from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from quacky_denue.config import PipelineConfig
from quacky_denue.pipeline import run_pipeline


@patch("quacky_denue.discovery.sync_playwright")
@patch("quacky_denue.download.urlopen")
def test_pipeline_single_year(mock_urlopen, mock_playwright, tmp_path: Path):
    # Mock discovery
    fake_links = [{"href": "https://fake.url/denue_09_2010_csv.zip", "text": "CDMX 2010"}]
    mock_browser = mock_playwright.return_value.__enter__.return_value
    mock_page = mock_browser.new_page.return_value
    mock_page.query_selector_all.return_value = [
        type("MockAnchor", (), {"get_attribute": lambda _, a: l["href"], "inner_text": lambda: l["text"]})()
        for l in fake_links
    ]
    mock_page.inner_text.return_value = "3"

    # Mock download
    from io import BytesIO
    import zipfile
    import pandas as pd

    df = pd.DataFrame({
        "id": [1],
        "nom_estab": ["Test"],
        "codigo_act": ["123"],
        "cve_ent": ["09"],
        "entidad": ["CDMX"],
    })
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("denue_09_2010.csv", df.to_csv(index=False))
    buf.seek(0)
    mock_urlopen.return_value.__enter__.return_value = buf

    config = PipelineConfig(
        download_url="https://fake.url",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "test.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report.json",
        headless=True,
        chunk_size=1,
    )

    report = run_pipeline(config)
    assert report.processed_files == 1
    assert report.written_rows == 1
    assert report.completeness_ratio() == 1.0

    # Verify report file
    assert config.report_path.exists()
    payload = json.loads(config.report_path.read_text())
    assert payload["processed_files"] == 1
    assert payload["written_rows"] == 1


@patch("quacky_denue.discovery.sync_playwright")
@patch("quacky_denue.download.urlopen")
def test_pipeline_years_for_estado(mock_urlopen, mock_playwright, tmp_path: Path):
    # Mock discovery for federation 09 across multiple years
    fake_links = [
        {"href": "https://fake.url/denue_09_2010_csv.zip", "text": "CDMX 2010"},
        {"href": "https://fake.url/denue_09_2020_csv.zip", "text": "CDMX 2020"},
    ]
    mock_browser = mock_playwright.return_value.__enter__.return_value
    mock_page = mock_browser.new_page.return_value
    mock_page.query_selector_all.return_value = [
        type("MockAnchor", (), {"get_attribute": lambda _, a: l["href"], "inner_text": lambda: l["text"]})()
        for l in fake_links
    ]
    mock_page.inner_text.return_value = "4"

    # Mock two different CSVs
    from io import BytesIO
    import zipfile
    import pandas as pd

    def mock_response(url, *args, **kwargs):
        df1 = pd.DataFrame({
            "id": [1],
            "nom_estab": ["Test2010"],
            "codigo_act": ["123"],
            "cve_ent": ["09"],
            "entidad": ["CDMX"],
        })
        df2 = pd.DataFrame({
            "id": [2],
            "nom_estab": ["Test2020"],
            "codigo_act": ["456"],
            "cve_ent": ["09"],
            "entidad": ["CDMX"],
        })
        buf = BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            if "2010" in url:
                zf.writestr("denue_09_2010.csv", df1.to_csv(index=False))
            else:
                zf.writestr("denue_09_2020.csv", df2.to_csv(index=False))
        buf.seek(0)
        return type("MockResp", (), {"__enter__": lambda s: buf, "__exit__": lambda s, *a: None})()

    mock_urlopen.side_effect = mock_response

    config = PipelineConfig(
        download_url="https://fake.url",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "test.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report.json",
        headless=True,
        chunk_size=1,
        federation_filter={"09"},
    )

    report = run_pipeline(config)
    assert report.processed_files == 2
    assert report.written_rows == 2
    assert report.completeness_ratio() == 1.0
