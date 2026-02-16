from __future__ import annotations

import logging
import re
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

LOGGER = logging.getLogger(__name__)
YEAR_PATTERN = re.compile(r"(20\d{2})")


def infer_snapshot_period(zip_file: Path) -> str:
    year_match = YEAR_PATTERN.search(zip_file.name)
    if year_match:
        return year_match.group(1)

    with ZipFile(zip_file) as archive:
        metadata = [name for name in archive.namelist() if "metadatos_denue" in name.lower()]
        if metadata:
            with archive.open(metadata[0]) as f:
                first_line = f.readline().decode("latin1", errors="ignore").strip()
                first_line = first_line.replace("Identifier:", "").strip()
                normalized = first_line.replace(".", "_").replace("-", "_").lower()
                return normalized

    return "unknown"


def _select_data_csv(archive_names: list[str]) -> str:
    csv_files = [x for x in archive_names if x.lower().endswith(".csv")]
    candidates = [x for x in csv_files if "diccionario_de_datos" not in x.lower()]
    if not candidates:
        raise ValueError("No DENUE csv file found in zip")
    return sorted(candidates)[0]


def iter_denue_chunks(zip_file: Path, chunk_size: int):
    with ZipFile(zip_file) as archive:
        csv_member = _select_data_csv(archive.namelist())
        with archive.open(csv_member) as csv_handle:
            reader = pd.read_csv(csv_handle, chunksize=chunk_size, low_memory=True, encoding="latin1")
            for chunk in reader:
                yield chunk

    LOGGER.info("Finished chunked read for %s", zip_file)
