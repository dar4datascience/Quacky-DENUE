import logging
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from tqdm import tqdm

from denue_fetcher import DENUEFetcher
from denue_downloader import DENUEDownloader
from denue_parser import DENUEParser
from denue_ingestion import DENUEIngestion

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/denue_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class DENUEPipeline:
    def __init__(self, database_path: str = "./data/denue.duckdb",
                 cache_dir: str = "./cache/denue",
                 report_path: str = "./reports/denue_report.json"):
        self.database_path = database_path
        self.cache_dir = cache_dir
        self.report_path = Path(report_path)
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        
        Path("logs").mkdir(exist_ok=True)
        
        self.fetcher = DENUEFetcher()
        self.downloader = DENUEDownloader(cache_dir=cache_dir)
        self.parser = DENUEParser()
        
        self.stats = {
            'execution_timestamp': datetime.utcnow().isoformat() + 'Z',
            'total_datasets_detected': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'successful_ingestions': 0,
            'failed_ingestions': 0,
            'failed_datasets': []
        }
    
    def run(self, limit: int = None) -> Dict:
        logger.info("=" * 80)
        logger.info("Starting DENUE Pipeline")
        logger.info("=" * 80)
        
        try:
            datasets = self.fetcher.fetch_datasets()
            
            if limit:
                datasets = datasets[:limit]
                logger.info(f"Limited to first {limit} datasets for testing")
            
            self.stats['total_datasets_detected'] = len(datasets)
            logger.info(f"Detected {len(datasets)} datasets to process")
            
            with DENUEIngestion(self.database_path) as ingestion:
                ingestion.initialize_database()
                
                with tqdm(datasets, desc="Processing datasets", unit="dataset") as pbar:
                    for dataset in pbar:
                        pbar.set_description(f"Processing {dataset.sector[:30]}")
                        self._process_dataset(dataset, ingestion)
                
                db_stats = ingestion.get_statistics()
                self.stats.update(db_stats)
                self.stats['failed_datasets'] = ingestion.get_failed_datasets()
            
            self._save_report()
            self._print_summary()
            
            logger.info("=" * 80)
            logger.info("DENUE Pipeline completed successfully")
            logger.info("=" * 80)
            
            return self.stats
            
        except Exception as e:
            logger.critical(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    def _process_dataset(self, dataset, ingestion):
        sector = dataset.sector
        period = dataset.period
        
        try:
            if ingestion.is_dataset_ingested(sector, period):
                logger.info(f"Skipping {sector} ({period}) - already ingested")
                self.stats['successful_ingestions'] += 1
                return
            
            zip_path = self.downloader.download_dataset(
                dataset.download_url, sector, period
            )
            
            if not zip_path:
                self.stats['failed_downloads'] += 1
                self.stats['failed_datasets'].append({
                    'sector': sector,
                    'periodo': period,
                    'error': 'Download failed',
                    'url': dataset.download_url
                })
                return
            
            self.stats['successful_downloads'] += 1
            
            extracted_paths = self.downloader.extract_dataset(zip_path)
            if not extracted_paths:
                self.stats['failed_ingestions'] += 1
                self.stats['failed_datasets'].append({
                    'sector': sector,
                    'periodo': period,
                    'error': 'Extraction failed',
                    'url': dataset.download_url
                })
                return
            
            schema = self.parser.parse_schema(extracted_paths['diccionario_de_datos'])
            if not schema:
                self.stats['failed_ingestions'] += 1
                self.stats['failed_datasets'].append({
                    'sector': sector,
                    'periodo': period,
                    'error': 'Schema parsing failed',
                    'url': dataset.download_url
                })
                return
            
            metadata = self.parser.parse_metadata(
                extracted_paths['metadatos'], sector, period,
                dataset.download_url, dataset.file_size
            )
            
            df = self.parser.parse_dataset(extracted_paths['conjunto_de_datos'], schema)
            if df is None:
                self.stats['failed_ingestions'] += 1
                self.stats['failed_datasets'].append({
                    'sector': sector,
                    'periodo': period,
                    'error': 'Dataset parsing failed',
                    'url': dataset.download_url
                })
                return
            
            is_valid, errors = self.parser.validate_dataset(df, schema)
            if not is_valid:
                logger.warning(f"Validation warnings for {sector}: {errors}")
            
            success = ingestion.ingest_dataset(df, metadata or {}, sector, period)
            
            if success:
                self.stats['successful_ingestions'] += 1
            else:
                self.stats['failed_ingestions'] += 1
                
        except Exception as e:
            logger.error(f"Error processing {sector} ({period}): {e}")
            self.stats['failed_ingestions'] += 1
            self.stats['failed_datasets'].append({
                'sector': sector,
                'periodo': period,
                'error': str(e),
                'url': dataset.download_url
            })
    
    def _save_report(self):
        try:
            with open(self.report_path, 'w') as f:
                json.dump(self.stats, f, indent=2)
            logger.info(f"Report saved to {self.report_path}")
        except Exception as e:
            logger.error(f"Error saving report: {e}")
    
    def _print_summary(self):
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Total datasets detected:     {self.stats['total_datasets_detected']}")
        print(f"Successful downloads:        {self.stats['successful_downloads']}")
        print(f"Failed downloads:            {self.stats['failed_downloads']}")
        print(f"Successful ingestions:       {self.stats['successful_ingestions']}")
        print(f"Failed ingestions:           {self.stats['failed_ingestions']}")
        print(f"Total records ingested:      {self.stats.get('total_records_ingested', 0):,}")
        print(f"Database size:               {self.stats.get('total_size_duckdb_mb', 0):.2f} MB")
        print(f"Compression ratio:           {self.stats.get('compression_ratio', 0):.2%}")
        
        if self.stats['failed_datasets']:
            print(f"\nFailed datasets ({len(self.stats['failed_datasets'])}):")
            for failed in self.stats['failed_datasets'][:5]:
                print(f"  - {failed['sector']} ({failed['periodo']}): {failed['error']}")
            if len(self.stats['failed_datasets']) > 5:
                print(f"  ... and {len(self.stats['failed_datasets']) - 5} more")
        
        print("=" * 80 + "\n")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='DENUE Data Ingestion Pipeline')
    parser.add_argument('--database', default='./data/denue.duckdb',
                       help='Path to DuckDB database')
    parser.add_argument('--cache', default='./cache/denue',
                       help='Cache directory for downloads')
    parser.add_argument('--report', default='./reports/denue_report.json',
                       help='Path to output report')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of datasets to process (for testing)')
    
    args = parser.parse_args()
    
    pipeline = DENUEPipeline(
        database_path=args.database,
        cache_dir=args.cache,
        report_path=args.report
    )
    
    pipeline.run(limit=args.limit)


if __name__ == '__main__':
    main()
