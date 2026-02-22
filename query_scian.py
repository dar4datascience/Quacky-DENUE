#!/usr/bin/env python
"""
Quick script to query SCIAN data from cached DuckDB.
"""
import duckdb
from pathlib import Path

db_path = Path("cache/scian.duckdb")

if not db_path.exists():
    print(f"Database not found at {db_path}")
    print("Run the ETL pipeline first to create the database.")
    exit(1)

conn = duckdb.connect(str(db_path))

print("="*80)
print("TOP 10 HIGHEST CÓDIGO VALUES FROM SCIAN 2023")
print("="*80)

result = conn.execute("""
    SELECT "Código", "Título", "Descripción"
    FROM scian_2023
    WHERE "Código" IS NOT NULL
    ORDER BY CAST("Código" AS INTEGER) DESC
    LIMIT 10
""").fetchall()

for codigo, titulo, descripcion in result:
    print(f"\nCódigo: {codigo}")
    print(f"Título: {titulo}")
    if descripcion:
        print(f"Descripción: {descripcion[:100]}...")

print("\n" + "="*80)
print("TABLE STATISTICS")
print("="*80)

tables = conn.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main'
""").fetchall()

for (table_name,) in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"{table_name}: {count:,} rows")

conn.close()
