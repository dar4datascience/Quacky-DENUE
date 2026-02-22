import pytest
import logging
from pathlib import Path
import sys
import tempfile
import shutil

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from denue_downloader import DENUEDownloader
from denue_fetcher import DENUEFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDENUEDownloader:
    
    @pytest.fixture
    def temp_cache_dir(self):
        temp_dir = tempfile.mkdtemp(prefix="test_denue_cache_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def sample_dataset(self):
        fetcher = DENUEFetcher(headless=True)
        datasets = fetcher.fetch_datasets()
        assert len(datasets) > 0, "Need at least one dataset for testing"
        return datasets[0]
    
    def test_download_dataset_live(self, temp_cache_dir, sample_dataset):
        downloader = DENUEDownloader(cache_dir=temp_cache_dir, timeout=120)
        
        zip_path = downloader.download_dataset(
            sample_dataset.download_url,
            sample_dataset.sector,
            sample_dataset.period
        )
        
        assert zip_path is not None, "Download should succeed"
        assert zip_path.exists(), "Downloaded file should exist"
        assert zip_path.suffix == '.zip', "Downloaded file should be a ZIP"
        assert zip_path.stat().st_size > 0, "Downloaded file should not be empty"
        
        logger.info(f"Downloaded file: {zip_path.name} ({zip_path.stat().st_size / 1024 / 1024:.2f} MB)")
    
    def test_download_caching(self, temp_cache_dir, sample_dataset):
        downloader = DENUEDownloader(cache_dir=temp_cache_dir)
        
        zip_path_1 = downloader.download_dataset(
            sample_dataset.download_url,
            sample_dataset.sector,
            sample_dataset.period
        )
        
        assert zip_path_1 is not None
        first_download_time = zip_path_1.stat().st_mtime
        
        zip_path_2 = downloader.download_dataset(
            sample_dataset.download_url,
            sample_dataset.sector,
            sample_dataset.period
        )
        
        assert zip_path_2 is not None
        assert zip_path_1 == zip_path_2, "Should return same cached file"
        assert zip_path_2.stat().st_mtime == first_download_time, "File should not be re-downloaded"
        
        logger.info("Cache working correctly - file not re-downloaded")
    
    def test_extract_dataset_live(self, temp_cache_dir, sample_dataset):
        downloader = DENUEDownloader(cache_dir=temp_cache_dir)
        
        zip_path = downloader.download_dataset(
            sample_dataset.download_url,
            sample_dataset.sector,
            sample_dataset.period
        )
        
        assert zip_path is not None
        
        extracted_paths = downloader.extract_dataset(zip_path)
        
        assert extracted_paths is not None, "Extraction should succeed"
        assert 'conjunto_de_datos' in extracted_paths
        assert 'diccionario_de_datos' in extracted_paths
        assert 'metadatos' in extracted_paths
        
        assert extracted_paths['conjunto_de_datos'].exists()
        assert extracted_paths['diccionario_de_datos'].exists()
        assert extracted_paths['metadatos'].exists()
        
        assert extracted_paths['conjunto_de_datos'].suffix == '.csv'
        assert extracted_paths['diccionario_de_datos'].suffix == '.csv'
        assert extracted_paths['metadatos'].suffix == '.txt'
        
        logger.info(f"Extracted files:")
        logger.info(f"  - conjunto_de_datos: {extracted_paths['conjunto_de_datos'].name}")
        logger.info(f"  - diccionario_de_datos: {extracted_paths['diccionario_de_datos'].name}")
        logger.info(f"  - metadatos: {extracted_paths['metadatos'].name}")
    
    def test_extract_caching(self, temp_cache_dir, sample_dataset):
        downloader = DENUEDownloader(cache_dir=temp_cache_dir)
        
        zip_path = downloader.download_dataset(
            sample_dataset.download_url,
            sample_dataset.sector,
            sample_dataset.period
        )
        
        extracted_paths_1 = downloader.extract_dataset(zip_path)
        assert extracted_paths_1 is not None
        
        extracted_paths_2 = downloader.extract_dataset(zip_path)
        assert extracted_paths_2 is not None
        
        assert extracted_paths_1 == extracted_paths_2, "Should return same cached extraction"
        
        logger.info("Extraction cache working correctly")
    
    def test_download_retry_on_invalid_url(self, temp_cache_dir):
        downloader = DENUEDownloader(cache_dir=temp_cache_dir, max_retries=2, timeout=5)
        
        zip_path = downloader.download_dataset(
            "https://www.inegi.org.mx/invalid_url.zip",
            "Test Sector",
            "01/2025"
        )
        
        assert zip_path is None, "Should return None for invalid URL after retries"
