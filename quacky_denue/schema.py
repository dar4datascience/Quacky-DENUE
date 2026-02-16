from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable

import pandas as pd

from quacky_denue.constants import CANONICAL_COLUMNS, COLUMN_ALIASES, REQUIRED_MINIMUM_COLUMNS

LOGGER = logging.getLogger(__name__)

NON_ALNUM = re.compile(r"[^a-zA-Z0-9]+")


def to_snake_case(text: str) -> str:
    cleaned = NON_ALNUM.sub("_", text.strip()).strip("_")
    return cleaned.lower()


def resolve_columns(columns: Iterable[str]) -> tuple[list[str], list[str]]:
    resolved: list[str] = []
    unknown: list[str] = []

    for col in columns:
        c = to_snake_case(col)
        canonical = COLUMN_ALIASES.get(c, c)
        resolved.append(canonical)
        if canonical not in CANONICAL_COLUMNS:
            unknown.append(canonical)

    return resolved, sorted(set(unknown))


def normalize_chunk(
    df: pd.DataFrame,
    *,
    snapshot_period: str,
    source_file: str,
    federation: str,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    resolved, unknown_cols = resolve_columns(df.columns)
    df.columns = resolved

    missing_required = [col for col in REQUIRED_MINIMUM_COLUMNS if col not in df.columns]

    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # capture unknown columns in a single JSON payload column to preserve data
    extras = [c for c in df.columns if c not in CANONICAL_COLUMNS]
    if extras:
        df["raw_extra_json"] = df[extras].fillna("").to_dict(orient="records")
        df["raw_extra_json"] = df["raw_extra_json"].map(json.dumps)

    df["snapshot_period"] = snapshot_period
    df["source_file"] = source_file
    df["federation"] = federation
    df["schema_version"] = "v1"
    df["extraction_ts"] = pd.Timestamp.now("UTC").isoformat()

    standardized = df.loc[:, list(CANONICAL_COLUMNS)].astype("string")
    return standardized, missing_required, unknown_cols
