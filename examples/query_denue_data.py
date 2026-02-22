#!/usr/bin/env python3
"""
Example queries for the DENUE database.

Demonstrates how to query the ingested data using DuckDB.
"""

import duckdb
from pathlib import Path


def main():
    db_path = "./data/denue.duckdb"
    
    if not Path(db_path).exists():
        print(f"Database not found at {db_path}")
        print("Run the pipeline first: python src/denue_pipeline.py")
        return
    
    conn = duckdb.connect(db_path)
    
    print("=" * 80)
    print("DENUE Database Queries")
    print("=" * 80)
    print()
    
    print("1. Total businesses in database:")
    result = conn.execute("SELECT COUNT(*) FROM denue").fetchone()
    print(f"   {result[0]:,} businesses")
    print()
    
    print("2. Ingestion status:")
    result = conn.execute("""
        SELECT 
            sector, 
            periodo_consulta, 
            record_count, 
            status,
            ingestion_timestamp
        FROM ingestion_log 
        ORDER BY ingestion_timestamp DESC
        LIMIT 10
    """).fetchall()
    
    print(f"   {'Sector':<30} {'Period':<10} {'Records':<10} {'Status':<10}")
    print("   " + "-" * 70)
    for row in result:
        print(f"   {row[0]:<30} {row[1]:<10} {row[2]:<10} {row[3]:<10}")
    print()
    
    print("3. Top 10 states by business count:")
    result = conn.execute("""
        SELECT entidad, COUNT(*) as count 
        FROM denue 
        GROUP BY entidad 
        ORDER BY count DESC 
        LIMIT 10
    """).fetchall()
    
    for i, (state, count) in enumerate(result, 1):
        print(f"   {i:2}. {state:<30} {count:>10,} businesses")
    print()
    
    print("4. Top 10 economic activities:")
    result = conn.execute("""
        SELECT nombre_act, COUNT(*) as count 
        FROM denue 
        GROUP BY nombre_act 
        ORDER BY count DESC 
        LIMIT 10
    """).fetchall()
    
    for i, (activity, count) in enumerate(result, 1):
        activity_short = activity[:50] + "..." if len(activity) > 50 else activity
        print(f"   {i:2}. {activity_short:<53} {count:>10,}")
    print()
    
    print("5. Database statistics:")
    stats = conn.execute("""
        SELECT 
            (SELECT COUNT(*) FROM denue) as total_records,
            (SELECT COUNT(DISTINCT cve_ent) FROM denue) as states,
            (SELECT COUNT(DISTINCT cve_mun) FROM denue) as municipalities,
            (SELECT COUNT(DISTINCT codigo_act) FROM denue) as economic_activities
    """).fetchone()
    
    print(f"   Total records:        {stats[0]:>10,}")
    print(f"   States:               {stats[1]:>10,}")
    print(f"   Municipalities:       {stats[2]:>10,}")
    print(f"   Economic activities:  {stats[3]:>10,}")
    print()
    
    print("=" * 80)
    
    conn.close()


if __name__ == '__main__':
    main()
