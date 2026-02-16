from __future__ import annotations

import pandas as pd
import pytest

from quacky_denue.schema import normalize_chunk, resolve_columns, to_snake_case


def test_to_snake_case():
    assert to_snake_case("Nombre De La Actividad") == "nombre_de_la_actividad"
    assert to_snake_case("codigo_act") == "codigo_act"
    assert to_snake_case("  Tipo  Vial  ") == "tipo_vial"


def test_resolve_columns():
    cols = ["id", "nom_estab", "codigo_act", "Nombre De La Actividad", "unknown_col"]
    resolved, unknown = resolve_columns(cols)
    assert resolved == ["id", "nom_estab", "codigo_act", "nombre_act", "unknown_col"]
    assert unknown == ["unknown_col"]


def test_normalize_chunk():
    df = pd.DataFrame({
        "id": [1, 2],
        "nom_estab": ["A", "B"],
        "codigo_act": ["123", "456"],
        "cve_ent": ["09", "09"],
        "entidad": ["CDMX", "CDMX"],
        "extra_field": ["x", "y"],
    })

    normalized, missing_required, unknown_cols = normalize_chunk(
        df,
        snapshot_period="2024",
        source_file="test.csv",
        federation="09",
    )

    assert "id" in normalized.columns
    assert "nom_estab" in normalized.columns
    assert "codigo_act" in normalized.columns
    assert "snapshot_period" in normalized.columns
    assert "source_file" in normalized.columns
    assert "federation" in normalized.columns
    assert "schema_version" in normalized.columns
    assert "extraction_ts" in normalized.columns

    assert missing_required == []
    assert unknown_cols == ["extra_field"]
    assert "raw_extra_json" in normalized.columns
