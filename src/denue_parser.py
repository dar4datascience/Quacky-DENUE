import logging
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


class DENUEParser:
    ENCODING = 'ISO-8859-3'
    
    def __init__(self):
        pass
    
    def parse_schema(self, diccionario_path: Optional[Path]) -> Optional[List[str]]:
        if diccionario_path is None:
            logger.info("No data dictionary provided, will infer schema from CSV")
            return None
        
        try:
            logger.info(f"Parsing schema from {diccionario_path.name}")
            
            df = pd.read_csv(diccionario_path, encoding=self.ENCODING, skiprows=1)
            
            if 'Nombre del Atributo en csv' not in df.columns:
                logger.error(f"Column 'Nombre del Atributo en csv' not found in {diccionario_path}")
                return None
            
            columns = df['Nombre del Atributo en csv'].dropna().tolist()
            columns = [self._to_snake_case(col) for col in columns]
            
            logger.info(f"Parsed {len(columns)} columns from schema")
            return columns
            
        except Exception as e:
            logger.error(f"Error parsing schema from {diccionario_path}: {e}")
            return None
    
    def parse_metadata(self, metadatos_path: Optional[Path], sector: str, period: str, 
                      download_url: str, file_size: str) -> Optional[Dict]:
        metadata = {
            'sector': sector,
            'periodo_consulta': period,
            'download_url': download_url,
            'file_size': file_size
        }
        
        if metadatos_path is None:
            logger.info("No metadata file provided, using basic metadata")
            return metadata
        
        try:
            logger.info(f"Parsing metadata from {metadatos_path.name}")
            
            content = None
            for encoding in [self.ENCODING, 'utf-8', 'latin-1']:
                try:
                    with open(metadatos_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    logger.debug(f"Successfully read metadata with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                logger.error(f"Could not decode metadata file with any known encoding")
                return metadata
            
            patterns = {
                'identifier': r'Identificador:\s*(.+)',
                'title': r'Título:\s*(.+)',
                'description': r'Descripción:\s*(.+)',
                'modified': r'Fecha de actualización:\s*(.+)',
                'publisher': r'Publicador:\s*(.+)',
                'temporal': r'Cobertura temporal:\s*(.+)',
                'spatial': r'Cobertura espacial:\s*(.+)',
                'accrual_periodicity': r'Frecuencia de actualización:\s*(.+)'
            }
            
            for key, pattern in patterns.items():
                match = re.search(pattern, content)
                if match:
                    metadata[key] = match.group(1).strip()
            
            logger.info(f"Successfully parsed metadata")
            return metadata
            
        except Exception as e:
            logger.error(f"Error parsing metadata from {metadatos_path}: {e}")
            return metadata
    
    def parse_dataset(self, conjunto_path: Path, expected_schema: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        try:
            logger.info(f"Parsing dataset from {conjunto_path.name}")
            
            df = None
            for encoding in [self.ENCODING, 'utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(conjunto_path, encoding=encoding, low_memory=False)
                    logger.debug(f"Successfully read CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                logger.error(f"Could not decode CSV file with any known encoding")
                return None
            
            df.columns = [self._to_snake_case(col) for col in df.columns]
            
            if expected_schema:
                missing_cols = set(expected_schema) - set(df.columns)
                extra_cols = set(df.columns) - set(expected_schema)
                
                if missing_cols:
                    logger.warning(f"Missing columns in dataset: {missing_cols}")
                if extra_cols:
                    logger.warning(f"Extra columns in dataset: {extra_cols}")
            else:
                logger.info(f"No expected schema provided, using columns from CSV")
            
            logger.info(f"Successfully parsed dataset with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error parsing dataset from {conjunto_path}: {e}")
            return None
    
    def validate_dataset(self, df: pd.DataFrame, expected_schema: Optional[List[str]] = None) -> Tuple[bool, List[str]]:
        errors = []
        
        if expected_schema:
            missing_cols = set(expected_schema) - set(df.columns)
            if missing_cols:
                errors.append(f"Missing required columns: {missing_cols}")
        
        if df.empty:
            errors.append("Dataset is empty")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def _to_snake_case(self, text: str) -> str:
        text = text.strip()
        text = re.sub(r'[áàäâ]', 'a', text, flags=re.IGNORECASE)
        text = re.sub(r'[éèëê]', 'e', text, flags=re.IGNORECASE)
        text = re.sub(r'[íìïî]', 'i', text, flags=re.IGNORECASE)
        text = re.sub(r'[óòöô]', 'o', text, flags=re.IGNORECASE)
        text = re.sub(r'[úùüû]', 'u', text, flags=re.IGNORECASE)
        text = re.sub(r'ñ', 'n', text, flags=re.IGNORECASE)
        text = re.sub(r'[^a-zA-Z0-9]+', '_', text)
        text = text.lower()
        text = re.sub(r'_+', '_', text)
        text = text.strip('_')
        return text
