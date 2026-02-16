from __future__ import annotations

import pandas as pd
import pytest

from quacky_denue.storage import DuckDBWriter, ParquetWriter, choose_storage_backend


def test_duckdb_writer(tmp_path):
    db_path = tmp_path / "test.duckdb"
    writer = DuckDBWriter(db_path)
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

    written = writer.write(df, "test_table")
    assert written == 2

    # verify via connection
    import duckdb
    conn = duckdb.connect(str(db_path))
    result = conn.execute("SELECT COUNT(*) FROM test_table").fetchone()
    assert result[0] == 2
    conn.close()
    writer.close()


def test_parquet_writer(tmp_path):
    parquet_dir = tmp_path / "parquet"
    writer = ParquetWriter(parquet_dir)
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

    written = writer.write(df, "test_table")
    assert written == 2

    # verify file exists and can be read
    files = list((parquet_dir / "test_table").glob("*.parquet"))
    assert len(files) == 1
    df_read = pd.read_parquet(files[0])
    assert len(df_read) == 2
    writer.close()


def test_choose_storage_backend(tmp_path):
    duck = choose_storage_backend("duckdb", tmp_path / "test.duckdb", tmp_path / "parquet")
    assert isinstance(duck, DuckDBWriter)

    parquet = choose_storage_backend("parquet", tmp_path / "test.duckdb", tmp_path / "parquet")
    assert isinstance(parquet, ParquetWriter)

    with pytest.raises(ValueError):
        choose_storage_backend("unknown", tmp_path / "test.duckdb", tmp_path / "parquet")
