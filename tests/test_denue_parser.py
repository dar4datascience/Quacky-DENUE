import pytest
import logging
from pathlib import Path
import sys
import tempfile
import shutil

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from denue_parser import DENUEParser
from denue_downloader import DENUEDownloader
from denue_fetcher import DENUEFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDENUEParser:
    
    @pytest.fixture
    def temp_cache_dir(self):
        temp_dir = tempfile.mkdtemp(prefix="test_denue_parser_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def sample_extracted_files(self, temp_cache_dir):
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
        
        return extracted_paths, sample_dataset
    
    def test_parse_schema_live(self, sample_extracted_files):
        extracted_paths, _ = sample_extracted_files
        parser = DENUEParser()
        
        schema = parser.parse_schema(extracted_paths['diccionario_de_datos'])
        
        assert schema is not None, "Schema parsing should succeed"
        assert len(schema) > 0, "Schema should have columns"
        
        logger.info(f"Parsed schema with {len(schema)} columns")
        logger.info(f"First 10 columns: {schema[:10]}")
        
        for col in schema:
            assert isinstance(col, str), "Column names should be strings"
            assert col == col.lower(), "Column names should be lowercase (snake_case)"
            assert ' ' not in col, "Column names should not have spaces"
    
    def test_parse_metadata_live(self, sample_extracted_files):
        extracted_paths, sample_dataset = sample_extracted_files
        parser = DENUEParser()
        
        metadata = parser.parse_metadata(
            extracted_paths['metadatos'],
            sample_dataset.sector,
            sample_dataset.period,
            sample_dataset.download_url,
            sample_dataset.file_size
        )
        
        assert metadata is not None, "Metadata parsing should succeed"
        assert metadata['sector'] == sample_dataset.sector
        assert metadata['periodo_consulta'] == sample_dataset.period
        
        logger.info(f"Parsed metadata:")
        for key, value in metadata.items():
            logger.info(f"  {key}: {value}")
        
        expected_keys = ['sector', 'periodo_consulta', 'download_url', 'file_size']
        for key in expected_keys:
            assert key in metadata, f"Metadata should contain {key}"
    
    def test_parse_dataset_live(self, sample_extracted_files):
        extracted_paths, _ = sample_extracted_files
        parser = DENUEParser()
        
        schema = parser.parse_schema(extracted_paths['diccionario_de_datos'])
        assert schema is not None
        
        df = parser.parse_dataset(extracted_paths['conjunto_de_datos'], schema)
        
        assert df is not None, "Dataset parsing should succeed"
        assert len(df) > 0, "Dataset should have rows"
        assert len(df.columns) > 0, "Dataset should have columns"
        
        logger.info(f"Parsed dataset with {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"Dataset columns: {list(df.columns)[:10]}")
        logger.info(f"Sample row:\n{df.iloc[0].to_dict()}")
        
        for col in df.columns:
            assert col == col.lower(), "Column names should be lowercase"
    
    def test_validate_dataset_live(self, sample_extracted_files):
        extracted_paths, _ = sample_extracted_files
        parser = DENUEParser()
        
        schema = parser.parse_schema(extracted_paths['diccionario_de_datos'])
        df = parser.parse_dataset(extracted_paths['conjunto_de_datos'], schema)
        
        assert df is not None
        
        is_valid, errors = parser.validate_dataset(df, schema)
        
        logger.info(f"Validation result: {'VALID' if is_valid else 'INVALID'}")
        if errors:
            logger.warning(f"Validation errors: {errors}")
        
        assert df is not None and len(df) > 0, "Dataset should not be empty"
    
    def test_snake_case_conversion(self):
        parser = DENUEParser()
        
        test_cases = [
            ("Nombre del Atributo", "nombre_del_atributo"),
            ("Código Postal", "codigo_postal"),
            ("Razón Social", "razon_social"),
            ("Número Exterior", "numero_exterior"),
            ("AGEB", "ageb"),
            ("Clave Entidad", "clave_entidad"),
        ]
        
        for input_text, expected in test_cases:
            result = parser._to_snake_case(input_text)
            logger.info(f"'{input_text}' -> '{result}'")
            assert result == expected, f"Expected '{expected}', got '{result}'"
