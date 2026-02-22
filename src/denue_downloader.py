import logging
import os
import time
import zipfile
import hashlib
from pathlib import Path
from typing import Optional, Dict
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)


class DENUEDownloader:
    def __init__(self, cache_dir: str = "./cache/denue", timeout: int = 60, 
                 max_retries: int = 3, backoff_factor: int = 2):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
    def download_dataset(self, url: str, sector: str, period: str) -> Optional[Path]:
        cache_key = self._get_cache_key(url, sector, period)
        cached_path = self.cache_dir / f"{cache_key}.zip"
        
        if cached_path.exists() and cached_path.stat().st_size > 0:
            logger.info(f"Using cached file for {sector} ({period}): {cached_path}")
            return cached_path
        elif cached_path.exists():
            logger.warning(f"Removing invalid cached file: {cached_path}")
            cached_path.unlink()
        
        logger.info(f"Downloading {sector} ({period}) from {url}")
        
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, timeout=self.timeout, stream=True)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                
                with open(cached_path, 'wb') as f:
                    with tqdm(total=total_size, unit='B', unit_scale=True, 
                             desc=f"{sector[:30]}", leave=False) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                logger.info(f"Successfully downloaded {sector} ({period})")
                return cached_path
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout downloading {sector} (attempt {attempt}/{self.max_retries})")
                if cached_path.exists():
                    cached_path.unlink()
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download {sector} after {self.max_retries} attempts")
                    return None
                    
            except Exception as e:
                logger.error(f"Error downloading {sector}: {e}")
                if cached_path.exists():
                    cached_path.unlink()
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    return None
        
        return None
    
    def extract_dataset(self, zip_path: Path, extract_dir: Optional[Path] = None) -> Optional[Dict[str, Path]]:
        if extract_dir is None:
            extract_dir = self.cache_dir / "extracted" / zip_path.stem
        
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        if self._is_already_extracted(extract_dir):
            logger.info(f"Using cached extraction: {extract_dir}")
            return self._get_extracted_paths(extract_dir)
        
        try:
            logger.info(f"Extracting {zip_path.name}...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            logger.info(f"Successfully extracted to {extract_dir}")
            return self._get_extracted_paths(extract_dir)
            
        except Exception as e:
            logger.error(f"Error extracting {zip_path}: {e}")
            return None
    
    def _get_cache_key(self, url: str, sector: str, period: str) -> str:
        key_string = f"{url}_{sector}_{period}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _is_already_extracted(self, extract_dir: Path) -> bool:
        has_folders = all((extract_dir / d).exists() for d in ['conjunto_de_datos', 'diccionario_de_datos', 'metadatos'])
        has_single_csv = len(list(extract_dir.glob('*.csv'))) > 0
        return has_folders or has_single_csv
    
    def _get_extracted_paths(self, extract_dir: Path) -> Dict[str, Path]:
        conjunto = extract_dir / 'conjunto_de_datos'
        diccionario = extract_dir / 'diccionario_de_datos'
        metadatos = extract_dir / 'metadatos'
        
        if all([conjunto.exists(), diccionario.exists(), metadatos.exists()]):
            conjunto_csv = next(conjunto.glob('*.csv'), None)
            diccionario_csv = next(diccionario.glob('*.csv'), None)
            metadatos_txt = next(metadatos.glob('*.txt'), None)
            
            if all([conjunto_csv, diccionario_csv, metadatos_txt]):
                return {
                    'conjunto_de_datos': conjunto_csv,
                    'diccionario_de_datos': diccionario_csv,
                    'metadatos': metadatos_txt,
                    'structure_type': 'standard'
                }
        
        csv_files = list(extract_dir.glob('*.csv'))
        if csv_files:
            logger.info(f"Found single CSV structure with {len(csv_files)} file(s)")
            return {
                'conjunto_de_datos': csv_files[0],
                'diccionario_de_datos': None,
                'metadatos': None,
                'structure_type': 'single_csv'
            }
        
        logger.error(f"Could not determine structure in {extract_dir}")
        return None
