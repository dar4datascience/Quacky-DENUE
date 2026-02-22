import logging
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import re

logger = logging.getLogger(__name__)


class DENUEDataset:
    def __init__(self, sector: str, period: str, download_url: str, file_size: str):
        self.sector = sector.strip()
        self.period = period.strip()
        self.download_url = download_url
        self.file_size = file_size.strip()
        
    def to_dict(self) -> Dict[str, str]:
        return {
            'sector': self.sector,
            'period': self.period,
            'download_url': self.download_url,
            'file_size': self.file_size
        }
    
    def __repr__(self) -> str:
        return f"DENUEDataset(sector='{self.sector}', period='{self.period}', size={self.file_size})"


class DENUEFetcher:
    BASE_URL = "https://www.inegi.org.mx"
    DOWNLOAD_PAGE = "https://www.inegi.org.mx/app/descarga/?ti=6"
    
    def __init__(self, headless: bool = True, timeout: int = 60000):
        self.headless = headless
        self.timeout = timeout
        
    def fetch_datasets(self) -> List[DENUEDataset]:
        logger.info(f"Fetching DENUE datasets from {self.DOWNLOAD_PAGE}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            try:
                page = browser.new_page()
                page.set_default_timeout(self.timeout)
                
                logger.info("Navigating to INEGI download page...")
                page.goto(self.DOWNLOAD_PAGE)
                
                logger.info("Waiting for DENUE tab to load...")
                page.wait_for_selector('#denue', state='visible', timeout=self.timeout)
                
                logger.info("Extracting dataset rows...")
                rows = page.query_selector_all('tr[data-nivel="3"][data-agrupacion="denue"]')
                
                datasets = []
                for row in rows:
                    try:
                        dataset = self._parse_row(row)
                        if dataset:
                            datasets.append(dataset)
                            logger.debug(f"Found dataset: {dataset}")
                    except Exception as e:
                        logger.warning(f"Failed to parse row: {e}")
                        continue
                
                logger.info(f"Successfully fetched {len(datasets)} DENUE datasets")
                return datasets
                
            except PlaywrightTimeout as e:
                logger.error(f"Timeout while fetching datasets: {e}")
                raise
            except Exception as e:
                logger.error(f"Error fetching datasets: {e}")
                raise
            finally:
                browser.close()
    
    def _parse_row(self, row) -> Optional[DENUEDataset]:
        cells = row.query_selector_all('td')
        if len(cells) < 3:
            return None
        
        sector_div = cells[0].query_selector('div')
        if not sector_div:
            return None
        sector = sector_div.inner_text().strip()
        
        period = cells[1].inner_text().strip()
        
        csv_link = cells[2].query_selector('a[href$="csv.zip"]')
        if not csv_link:
            return None
        
        download_url = csv_link.get_attribute('href')
        if not download_url:
            return None
        
        if not download_url.startswith('http'):
            download_url = self.BASE_URL + download_url
        
        size_span = csv_link.query_selector('span')
        file_size = size_span.inner_text().strip() if size_span else "Unknown"
        
        return DENUEDataset(sector, period, download_url, file_size)
