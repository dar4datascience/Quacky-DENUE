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
    if not csv_files:
        raise ValueError("No DENUE csv file found in zip")

    conjunto_candidates = [
        x for x in csv_files if "conjunto" in x.lower() and "datos" in x.lower()
    ]
    if conjunto_candidates:
        return sorted(conjunto_candidates)[0]

    non_dictionary_candidates = [x for x in csv_files if "diccionario" not in x.lower()]
    if non_dictionary_candidates:
        return sorted(non_dictionary_candidates)[0]

    return sorted(csv_files)[0]


def _detect_text_encoding(sample_bytes: bytes) -> str:
    if sample_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    try:
        sample_bytes.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin1"


def _detect_csv_member_encoding(archive: ZipFile, csv_member: str) -> str:
    with archive.open(csv_member) as csv_handle:
        sample = csv_handle.read(64 * 1024)
    return _detect_text_encoding(sample)


def iter_denue_chunks(zip_file: Path, chunk_size: int):
    with ZipFile(zip_file) as archive:
        csv_member = _select_data_csv(archive.namelist())
        encoding = _detect_csv_member_encoding(archive, csv_member)
        with archive.open(csv_member) as csv_handle:
            reader = pd.read_csv(csv_handle, chunksize=chunk_size, low_memory=True, encoding=encoding)
            for chunk in reader:
                yield chunk

    LOGGER.info("Finished chunked read for %s using member=%s encoding=%s", zip_file, csv_member, encoding)
