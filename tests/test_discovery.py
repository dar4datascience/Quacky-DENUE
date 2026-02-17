from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

from quacky_denue.config import PipelineConfig
from quacky_denue.discovery import (
    _parse_state_code,
    _parse_federation,
    discover_denue_links,
    is_denue_csv_zip_url,
    validate_link_count,
)


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


def test_parse_state_code():
    assert _parse_state_code("AG_9", None) == "09"
    assert _parse_state_code(None, "15") == "15"
    assert _parse_state_code("AG_31", "31") == "31"
    assert _parse_state_code(None, None) is None


@pytest.mark.live
@pytest.mark.skipif(
    os.getenv("RUN_LIVE_DENUE_TESTS") != "1",
    reason="Set RUN_LIVE_DENUE_TESTS=1 to run live INEGI scraping tests",
)
def test_live_discover_denue_links_iterates_states(tmp_path: Path):
    config = PipelineConfig(
        download_url="https://www.inegi.org.mx/app/descarga/?ti=6",
        download_dir=tmp_path,
        storage_backend="duckdb",
        duckdb_path=tmp_path / "test.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report.json",
        headless=True,
        max_files=500,
    )

    links = discover_denue_links(config)

    assert len(links) > 0
    assert all(is_denue_csv_zip_url(link.href) for link in links)
    assert all(re.fullmatch(r"\d{2}(?:-\d{2})?", link.federation) for link in links)
    assert len({link.federation for link in links}) >= 20
    assert isinstance(validate_link_count(config, len(links)), bool)
