from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

import duckdb
import pandas as pd

from quacky_denue.config import PipelineConfig
from quacky_denue.models import DownloadLink
from quacky_denue.pipeline import run_pipeline


def _write_denue_zip(zip_path: Path, csv_name: str, rows: list[dict[str, str]]) -> Path:
    df = pd.DataFrame(rows)
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(csv_name, df.to_csv(index=False))
    return zip_path


@patch("quacky_denue.pipeline.validate_link_count", return_value=True)
@patch("quacky_denue.pipeline.download_zip")
@patch("quacky_denue.pipeline.discover_denue_links")
def test_pipeline_single_year_2010(mock_discover, mock_download, _mock_validate, tmp_path: Path):
    zip_2010 = _write_denue_zip(
        tmp_path / "fixtures" / "denue_09_2010_csv.zip",
        "denue_09_2010.csv",
        [
            {
                "id": "1",
                "nom_estab": "NEGOCIO_2010",
                "codigo_act": "461110",
                "cve_ent": "09",
                "entidad": "CIUDAD_DE_MEXICO",
            }
        ],
    )

    mock_discover.return_value = [
        DownloadLink(href="https://example/denue_09_2010_csv.zip", text="CDMX 2010", federation="09")
    ]
    mock_download.return_value = zip_2010

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

    assert config.report_path.exists()
    payload = json.loads(config.report_path.read_text())
    assert payload["processed_files"] == 1
    assert payload["written_rows"] == 1

    conn = duckdb.connect(str(config.duckdb_path))
    try:
        rows = conn.execute("SELECT COUNT(*) FROM denue_2010").fetchone()[0]
        assert rows == 1
    finally:
        conn.close()


@patch("quacky_denue.pipeline.validate_link_count", return_value=True)
@patch("quacky_denue.pipeline.download_zip")
@patch("quacky_denue.pipeline.discover_denue_links")
def test_pipeline_single_year_2020(mock_discover, mock_download, _mock_validate, tmp_path: Path):
    zip_2020 = _write_denue_zip(
        tmp_path / "fixtures" / "denue_09_2020_csv.zip",
        "denue_09_2020.csv",
        [
            {
                "id": "2",
                "nom_estab": "NEGOCIO_2020",
                "codigo_act": "722511",
                "cve_ent": "09",
                "entidad": "CIUDAD_DE_MEXICO",
            }
        ],
    )

    mock_discover.return_value = [
        DownloadLink(href="https://example/denue_09_2020_csv.zip", text="CDMX 2020", federation="09")
    ]
    mock_download.return_value = zip_2020

    config = PipelineConfig(
        download_url="https://fake.url",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "test_2020.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report_2020.json",
        headless=True,
        chunk_size=1,
    )

    report = run_pipeline(config)
    assert report.processed_files == 1
    assert report.written_rows == 1
    assert report.completeness_ratio() == 1.0

    conn = duckdb.connect(str(config.duckdb_path))
    try:
        rows = conn.execute("SELECT COUNT(*) FROM denue_2020").fetchone()[0]
        assert rows == 1
    finally:
        conn.close()


@patch("quacky_denue.pipeline.validate_link_count", return_value=True)
@patch("quacky_denue.pipeline.download_zip")
@patch("quacky_denue.pipeline.discover_denue_links")
def test_pipeline_all_years_for_estado(mock_discover, mock_download, _mock_validate, tmp_path: Path):
    zip_2019_1 = _write_denue_zip(
        tmp_path / "fixtures" / "denue_09_2019_csv.zip",
        "denue_09_2019.csv",
        [{"id": "10", "nom_estab": "A", "codigo_act": "461110", "cve_ent": "09", "entidad": "CDMX"}],
    )
    zip_2020_1 = _write_denue_zip(
        tmp_path / "fixtures" / "denue_09_2020_01_csv.zip",
        "denue_09_2020_01.csv",
        [{"id": "20", "nom_estab": "B", "codigo_act": "722511", "cve_ent": "09", "entidad": "CDMX"}],
    )
    zip_2020_2 = _write_denue_zip(
        tmp_path / "fixtures" / "denue_09_2020_07_csv.zip",
        "denue_09_2020_07.csv",
        [{"id": "21", "nom_estab": "C", "codigo_act": "722511", "cve_ent": "09", "entidad": "CDMX"}],
    )
    zip_2021_1 = _write_denue_zip(
        tmp_path / "fixtures" / "denue_09_2021_csv.zip",
        "denue_09_2021.csv",
        [{"id": "30", "nom_estab": "D", "codigo_act": "541110", "cve_ent": "09", "entidad": "CDMX"}],
    )

    links = [
        DownloadLink(href="https://example/denue_09_2019_csv.zip", text="CDMX 2019", federation="09"),
        DownloadLink(href="https://example/denue_09_2020_01_csv.zip", text="CDMX 2020-01", federation="09"),
        DownloadLink(href="https://example/denue_09_2020_07_csv.zip", text="CDMX 2020-07", federation="09"),
        DownloadLink(href="https://example/denue_09_2021_csv.zip", text="CDMX 2021", federation="09"),
    ]
    by_href = {
        links[0].href: zip_2019_1,
        links[1].href: zip_2020_1,
        links[2].href: zip_2020_2,
        links[3].href: zip_2021_1,
    }

    mock_discover.return_value = links
    mock_download.side_effect = lambda link, _download_dir: by_href[link.href]

    config = PipelineConfig(
        download_url="https://fake.url",
        download_dir=tmp_path / "downloads",
        storage_backend="duckdb",
        duckdb_path=tmp_path / "estado_09.duckdb",
        parquet_dir=tmp_path / "parquet",
        report_path=tmp_path / "report_estado_09.json",
        headless=True,
        chunk_size=1,
        federation_filter={"09"},
    )

    report = run_pipeline(config)
    assert report.processed_files == 4
    assert report.written_rows == 4
    assert report.completeness_ratio() == 1.0

    conn = duckdb.connect(str(config.duckdb_path))
    try:
        # Most years can have two cuts; 2020 has two fixture files in this test.
        rows_2019 = conn.execute("SELECT COUNT(*) FROM denue_2019").fetchone()[0]
        rows_2020 = conn.execute("SELECT COUNT(*) FROM denue_2020").fetchone()[0]
        rows_2021 = conn.execute("SELECT COUNT(*) FROM denue_2021").fetchone()[0]
        assert rows_2019 == 1
        assert rows_2020 == 2
        assert rows_2021 == 1
    finally:
        conn.close()
