#!/usr/bin/env python3
"""
Example script to run the DENUE data fetching pipeline.

This demonstrates how to use the pipeline with custom configuration.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from denue_pipeline import DENUEPipeline


def main():
    print("=" * 80)
    print("DENUE Data Fetching Pipeline - Example Run")
    print("=" * 80)
    print()
    
    pipeline = DENUEPipeline(
        database_path="./data/denue.duckdb",
        cache_dir="./cache/denue",
        report_path="./reports/denue_report.json"
    )
    
    print("Processing first 2 datasets as a test...")
    print()
    
    stats = pipeline.run(limit=2)
    
    print()
    print("Pipeline completed!")
    print(f"Check the report at: ./reports/denue_report.json")
    print(f"Query the database at: ./data/denue.duckdb")
    print()
    
    return stats


if __name__ == '__main__':
    main()
