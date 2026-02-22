#!/usr/bin/env python3
"""
Full DENUE Data Extraction Pipeline
Fetches, downloads, parses, and ingests all DENUE datasets from INEGI
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'src'))

from denue_pipeline import DENUEPipeline
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 80)
    logger.info("STARTING FULL DENUE EXTRACTION PIPELINE")
    logger.info("=" * 80)
    
    pipeline = DENUEPipeline(
        database_path='./data/denue.duckdb',
        cache_dir='./cache/denue',
        report_path='./reports/denue_full_extraction_report.json'
    )
    
    try:
        logger.info("Running pipeline for ALL datasets...")
        report = pipeline.run()
        
        logger.info("=" * 80)
        logger.info("PIPELINE EXECUTION COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Total datasets processed: {report['total_datasets']}")
        logger.info(f"Successful ingestions: {report['successful_ingestions']}")
        logger.info(f"Failed ingestions: {report['failed_ingestions']}")
        logger.info(f"Total records ingested: {report.get('total_records_ingested', 'N/A')}")
        logger.info(f"Report saved to: {pipeline.report_path}")
        logger.info(f"Database saved to: {pipeline.database_path}")
        logger.info("=" * 80)
        
        if report['failed_ingestions'] > 0:
            logger.warning(f"\n{report['failed_ingestions']} datasets failed to ingest:")
            for failed in report.get('failed_datasets', []):
                logger.warning(f"  - {failed['sector']} ({failed['periodo']}): {failed.get('error', 'Unknown error')}")
        
        return 0 if report['failed_ingestions'] == 0 else 1
        
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    sys.exit(main())
