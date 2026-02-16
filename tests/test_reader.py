from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from quacky_denue.reader import _select_data_csv, infer_snapshot_period, iter_denue_chunks


def test_infer_snapshot_period_from_filename(tmp_path: Path):
    zip_path = tmp_path / "denue_09_2020_csv.zip"
    zip_path.touch()
    assert infer_snapshot_period(zip_path) == "2020"


def test_infer_snapshot_period_fallback(tmp_path: Path):
    zip_path = tmp_path / "denue_09_unknown_csv.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("metadatos_denue.txt", "Identifier: snapshot_2021_01")
    assert infer_snapshot_period(zip_path) == "snapshot_2021_01"


def test_select_data_csv():
    names = ["diccionario_de_datos.csv", "denue_09_2020.csv", "extra.txt"]
    assert _select_data_csv(names) == "denue_09_2020.csv"


def test_iter_denue_chunks(tmp_path: Path):
    zip_path = tmp_path / "test.zip"
    df = pd.DataFrame({"id": [1, 2, 3], "nom_estab": ["A", "B", "C"]})
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("denue_09_2020.csv", df.to_csv(index=False))

    chunks = list(iter_denue_chunks(zip_path, chunk_size=2))
    assert len(chunks) == 2
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1
