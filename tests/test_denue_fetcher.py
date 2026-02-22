import pytest
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from denue_fetcher import DENUEFetcher, DENUEDataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDENUEFetcher:
    
    def test_fetch_datasets_live(self):
        fetcher = DENUEFetcher(headless=True, timeout=60000)
        
        datasets = fetcher.fetch_datasets()
        
        assert datasets is not None, "Datasets should not be None"
        assert len(datasets) > 0, "Should fetch at least one dataset"
        
        logger.info(f"Fetched {len(datasets)} datasets from INEGI")
        
        for dataset in datasets[:3]:
            logger.info(f"Sample dataset: {dataset}")
        
        first_dataset = datasets[0]
        assert isinstance(first_dataset, DENUEDataset), "Should return DENUEDataset objects"
        assert first_dataset.sector, "Sector should not be empty"
        assert first_dataset.period, "Period should not be empty"
        assert first_dataset.download_url, "Download URL should not be empty"
        assert first_dataset.download_url.startswith('http'), "Download URL should be absolute"
        assert first_dataset.file_size, "File size should not be empty"
    
    def test_dataset_to_dict(self):
        dataset = DENUEDataset(
            sector="Construcción",
            period="05/2025",
            download_url="https://www.inegi.org.mx/contenidos/masiva/denue/denue_00_23_csv.zip",
            file_size="2.22 MB"
        )
        
        data_dict = dataset.to_dict()
        
        assert data_dict['sector'] == "Construcción"
        assert data_dict['period'] == "05/2025"
        assert data_dict['download_url'].startswith('http')
        assert data_dict['file_size'] == "2.22 MB"
    
    def test_fetch_datasets_structure(self):
        fetcher = DENUEFetcher(headless=True)
        datasets = fetcher.fetch_datasets()
        
        assert len(datasets) > 10, "Should fetch multiple datasets (typically 20+)"
        
        periods = set(d.period for d in datasets)
        logger.info(f"Found periods: {periods}")
        
        sectors = set(d.sector for d in datasets)
        logger.info(f"Found {len(sectors)} unique sectors")
        assert len(sectors) > 5, "Should have multiple sectors"
        
        for dataset in datasets:
            assert dataset.period, "Period should not be empty"
            assert 'csv.zip' in dataset.download_url.lower(), "URL should point to CSV ZIP"
