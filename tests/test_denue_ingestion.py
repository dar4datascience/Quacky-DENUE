import pytest
import logging
from pathlib import Path
import sys
import tempfile
import shutil
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from denue_ingestion import DENUEIngestion
from denue_parser import DENUEParser
from denue_downloader import DENUEDownloader
from denue_fetcher import DENUEFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDENUEIngestion:
    
    @pytest.fixture
    def temp_db_path(self):
        temp_dir = tempfile.mkdtemp(prefix="test_denue_db_")
        db_path = Path(temp_dir) / "test_denue.duckdb"
        yield str(db_path)
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def temp_cache_dir(self):
        temp_dir = tempfile.mkdtemp(prefix="test_denue_cache_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def sample_data(self, temp_cache_dir):
        fetcher = DENUEFetcher(headless=True)
        datasets = fetcher.fetch_datasets()
        assert len(datasets) > 0
        
        sample_dataset = datasets[0]
        
        downloader = DENUEDownloader(cache_dir=temp_cache_dir)
        zip_path = downloader.download_dataset(
            sample_dataset.download_url,
            sample_dataset.sector,
            sample_dataset.period
        )
        
        assert zip_path is not None
        
        extracted_paths = downloader.extract_dataset(zip_path)
        assert extracted_paths is not None
        
        parser = DENUEParser()
        schema = parser.parse_schema(extracted_paths['diccionario_de_datos'])
        metadata = parser.parse_metadata(
            extracted_paths['metadatos'],
            sample_dataset.sector,
            sample_dataset.period,
            sample_dataset.download_url,
            sample_dataset.file_size
        )
        df = parser.parse_dataset(extracted_paths['conjunto_de_datos'], schema)
        
        return df, metadata, sample_dataset
    
    def test_initialize_database(self, temp_db_path):
        with DENUEIngestion(temp_db_path) as ingestion:
            ingestion.initialize_database()
            
            tables = ingestion.conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            
            table_names = [t[0] for t in tables]
            
            assert 'denue' in table_names, "Should create denue table"
            assert 'ingestion_log' in table_names, "Should create ingestion_log table"
            
            logger.info(f"Created tables: {table_names}")
    
    def test_ingest_dataset_live(self, temp_db_path, sample_data):
        df, metadata, sample_dataset = sample_data
        
        with DENUEIngestion(temp_db_path) as ingestion:
            ingestion.initialize_database()
            
            success = ingestion.ingest_dataset(
                df, metadata, sample_dataset.sector, sample_dataset.period
            )
            
            assert success, "Ingestion should succeed"
            
            count = ingestion.conn.execute("SELECT COUNT(*) FROM denue").fetchone()[0]
            assert count > 0, "Should have ingested records"
            
            logger.info(f"Ingested {count} records from {sample_dataset.sector}")
            
            log_count = ingestion.conn.execute("""
                SELECT COUNT(*) FROM ingestion_log WHERE status = 'success'
            """).fetchone()[0]
            assert log_count == 1, "Should have one successful ingestion log"
    
    def test_ingestion_caching(self, temp_db_path, sample_data):
        df, metadata, sample_dataset = sample_data
        
        with DENUEIngestion(temp_db_path) as ingestion:
            ingestion.initialize_database()
            
            success_1 = ingestion.ingest_dataset(
                df, metadata, sample_dataset.sector, sample_dataset.period
            )
            assert success_1
            
            count_after_first = ingestion.conn.execute("SELECT COUNT(*) FROM denue").fetchone()[0]
            
            is_ingested = ingestion.is_dataset_ingested(sample_dataset.sector, sample_dataset.period)
            assert is_ingested, "Should detect dataset is already ingested"
            
            success_2 = ingestion.ingest_dataset(
                df, metadata, sample_dataset.sector, sample_dataset.period
            )
            assert success_2
            
            count_after_second = ingestion.conn.execute("SELECT COUNT(*) FROM denue").fetchone()[0]
            
            assert count_after_second == count_after_first, "Should not duplicate records"
            
            logger.info("Ingestion caching working correctly - no duplicate records")
    
    def test_get_statistics(self, temp_db_path, sample_data):
        df, metadata, sample_dataset = sample_data
        
        with DENUEIngestion(temp_db_path) as ingestion:
            ingestion.initialize_database()
            
            ingestion.ingest_dataset(df, metadata, sample_dataset.sector, sample_dataset.period)
            
            stats = ingestion.get_statistics()
            
            assert 'total_records_ingested' in stats
            assert 'successful_ingestions' in stats
            assert 'failed_ingestions' in stats
            assert 'total_size_duckdb_mb' in stats
            assert 'compression_ratio' in stats
            
            assert stats['total_records_ingested'] > 0
            assert stats['successful_ingestions'] == 1
            assert stats['total_size_duckdb_mb'] > 0
            
            logger.info(f"Database statistics:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
    
    def test_get_failed_datasets(self, temp_db_path):
        with DENUEIngestion(temp_db_path) as ingestion:
            ingestion.initialize_database()
            
            ingestion._log_ingestion("Test Sector", "01/2025", 0, 0, 'failed', 'Test error')
            
            failed = ingestion.get_failed_datasets()
            
            assert len(failed) == 1
            assert failed[0]['sector'] == "Test Sector"
            assert failed[0]['periodo'] == "01/2025"
            assert failed[0]['error'] == "Test error"
            
            logger.info(f"Failed datasets: {failed}")
