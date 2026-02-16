from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd

LOGGER = logging.getLogger(__name__)


class StorageWriter:
    def write(self, df: pd.DataFrame, table_name: str) -> int:
        raise NotImplementedError

    def close(self) -> None:
        return


class DuckDBWriter(StorageWriter):
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = duckdb.connect(str(db_path))

    def write(self, df: pd.DataFrame, table_name: str) -> int:
        self.conn.register("incoming_chunk", df)
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM incoming_chunk LIMIT 0"
        )
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM incoming_chunk")
        self.conn.unregister("incoming_chunk")
        return len(df)

    def close(self) -> None:
        self.conn.close()


class ParquetWriter(StorageWriter):
    def __init__(self, parquet_dir: Path):
        self.parquet_dir = parquet_dir
        self.parquet_dir.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame, table_name: str) -> int:
        table_dir = self.parquet_dir / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        output_file = table_dir / f"chunk_{uuid4().hex}.parquet"
        df.to_parquet(output_file, index=False)
        return len(df)


def choose_storage_backend(storage_backend: str, duckdb_path: Path, parquet_dir: Path) -> StorageWriter:
    backend = storage_backend.lower().strip()

    if backend == "duckdb":
        LOGGER.info("Using DuckDB storage backend for memory-efficient append and analytics")
        return DuckDBWriter(duckdb_path)

    if backend == "parquet":
        LOGGER.info("Using Parquet storage backend (good interchange, weaker append semantics)")
        return ParquetWriter(parquet_dir)

    raise ValueError(f"Unsupported storage backend: {storage_backend}")
