from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import patch

import pytest

from quacky_denue.config import PipelineConfig
from quacky_denue.discovery import _parse_federation, discover_denue_links, validate_link_count
from quacky_denue.models import DownloadLink

FEDERATION_PATTERN = re.compile(r"denue_([0-9]{1,2}(?:-[0-9]{1,2})?)_", re.IGNORECASE)


def test_parse_federation():
    assert _parse_federation("denue_09_2024_csv.zip", "CDMX") == "09"
    assert _parse_federation("denue_31-32_2023_csv.zip", "Yucatán") == "31-32"
    assert _parse_federation("some_other_file.zip", "Other") == "Other"


@patch("quacky_denue.discovery.sync_playwright")
def test_discover_denue_links(mock_playwright, tmp_path: Path):
    fake_links = [
        {"href": "https://fake.url/denue_09_2024_csv.zip", "text": "CDMX 2024"},
        {"href": "https://fake.url/denue_15_2024_csv.zip", "text": "México 2024"},
        {"href": "https://fake.url/other_file.zip", "text": "Other"},
    ]

    mock_browser = mock_playwright.return_value.__enter__.return_value
    mock_page = mock_browser.new_page.return_value
    mock_page.query_selector_all.return_value = [
        type("MockAnchor", (), {"get_attribute": lambda _, a: l["href"], "inner_text": lambda: l["text"]})()
        for l in fake_links
    ]

    config = PipelineConfig(
        download_url="https://fake.url",
        download_dir=tmp_path,
        storage_backend="duckdb",
        duckdb_path=tmp_path / "test.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report.json",
        headless=True,
    )

    links = discover_denue_links(config)
    assert len(links) == 2
    assert all(isinstance(l, DownloadLink) for l in links)
    assert {l.federation for l in links} == {"09", "15"}


@patch("quacky_denue.discovery.sync_playwright")
def test_validate_link_count(mock_playwright, tmp_path: Path):
    mock_browser = mock_playwright.return_value.__enter__.return_value
    mock_page = mock_browser.new_page.return_value
    mock_page.inner_text.return_value = "5"

    config = PipelineConfig(
        download_url="https://fake.url",
        download_dir=tmp_path,
        storage_backend="duckdb",
        duckdb_path=tmp_path / "test.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report.json",
        headless=True,
    )

    assert validate_link_count(config, 3) is True
    assert validate_link_count(config, 4) is True
    assert validate_link_count(config, 5) is False
