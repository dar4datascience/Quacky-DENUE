from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from quacky_denue.config import PipelineConfig
from quacky_denue.discovery import (
    _parse_federation,
    discover_denue_links,
    is_denue_csv_zip_url,
    validate_link_count,
)
from quacky_denue.models import DownloadLink


class MockAnchor:
    def __init__(self, href: str, text: str):
        self._href = href
        self._text = text

    def get_attribute(self, attr: str):
        if attr == "href":
            return self._href
        return None

    def inner_text(self) -> str:
        return self._text


def test_is_denue_csv_zip_url():
    assert is_denue_csv_zip_url(
        "https://www.inegi.org.mx/contenidos/masiva/denue/2010/denue_09_2010_csv.zip"
    )
    assert is_denue_csv_zip_url(
        "https://www.inegi.org.mx/contenidos/masiva/denue/2020/denue_09_2020_07_csv.zip"
    )
    assert is_denue_csv_zip_url(
        "https://www.inegi.org.mx/contenidos/masiva/denue/2015/denue_09_25022015_csv.zip"
    )
    assert not is_denue_csv_zip_url("https://www.inegi.org.mx/contenidos/masiva/denue/2010/readme.pdf")
    assert not is_denue_csv_zip_url("https://www.inegi.org.mx/contenidos/masiva/denue/2010/denue_2010_csv.zip")
    assert not is_denue_csv_zip_url("https://www.inegi.org.mx/contenidos/masiva/denue/2015/denue_09_25022015_shp.zip")


def test_parse_federation():
    assert _parse_federation("denue_09_2024_csv.zip", "CDMX") == "09"
    assert _parse_federation("denue_31-32_2023_csv.zip", "Yucatán") == "31-32"
    assert _parse_federation("some_other_file.zip", "Other") == "Other"


@patch("quacky_denue.discovery.sync_playwright")
def test_discover_denue_links(mock_playwright, tmp_path: Path):
    fake_links = [
        {
            "href": "/contenidos/masiva/denue/2010/denue_09_2010_csv.zip",
            "text": "CDMX 2010",
        },
        {
            "href": "/contenidos/masiva/denue/2020/denue_15_2020_07_csv.zip",
            "text": "México 2020",
        },
        {"href": "/contenidos/masiva/denue/2010/readme.pdf", "text": "Other"},
    ]

    mock_playwright_context = mock_playwright.return_value.__enter__.return_value
    mock_browser = mock_playwright_context.chromium.launch.return_value
    mock_context = mock_browser.new_context.return_value
    mock_page = mock_context.new_page.return_value
    mock_page.query_selector_all.return_value = [MockAnchor(l["href"], l["text"]) for l in fake_links]

    config = PipelineConfig(
        download_url="https://www.inegi.org.mx/app/descarga/?ti=6",
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
    mock_playwright_context = mock_playwright.return_value.__enter__.return_value
    mock_browser = mock_playwright_context.chromium.launch.return_value
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
    assert validate_link_count(config, 4) is False
    assert validate_link_count(config, 5) is False
