import os
import re
from pathlib import Path
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from prefect import task


INEGI_SCIAN_URL = "https://www.inegi.org.mx/scian/"
CACHE_DIR = Path(__file__).parent.parent / "cache"


@task(name="fetch_scian_page", retries=3, retry_delay_seconds=5)
def fetch_scian_page(url: str = INEGI_SCIAN_URL) -> str:
    """
    Fetch the INEGI SCIAN page HTML using Playwright for JavaScript rendering.
    
    Args:
        url: URL of the SCIAN page
        
    Returns:
        HTML content as string
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)
            html_content = page.content()
        finally:
            browser.close()
        
        return html_content


@task(name="parse_xlsx_links")
def parse_xlsx_links(html_content: str, base_url: str = "https://www.inegi.org.mx") -> List[Tuple[str, str]]:
    """
    Parse HTML to find all .xlsx download links.
    
    Args:
        html_content: HTML content to parse
        base_url: Base URL for resolving relative links
        
    Returns:
        List of tuples (link_text, full_url) for all .xlsx files found
    """
    soup = BeautifulSoup(html_content, 'lxml')
    xlsx_links = []
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.xlsx') or '.xlsx?' in href:
            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = base_url + href
            else:
                full_url = base_url + '/' + href
            
            link_text = link.get_text(strip=True) or "Unknown"
            xlsx_links.append((link_text, full_url))
    
    return xlsx_links


@task(name="download_scian_file", retries=2, retry_delay_seconds=10)
def download_scian_file(url: str, output_path: Path = None) -> Path:
    """
    Download SCIAN XLSX file to local cache.
    
    Args:
        url: URL of the XLSX file
        output_path: Optional custom output path
        
    Returns:
        Path to downloaded file
    """
    if output_path is None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        filename = url.split('/')[-1].split('?')[0]
        output_path = CACHE_DIR / filename
    
    response = requests.get(url, timeout=60, stream=True)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size = output_path.stat().st_size
    if file_size < 1_000_000:
        raise ValueError(f"Downloaded file is too small ({file_size} bytes). Expected > 1 MB.")
    
    return output_path


@task(name="validate_xlsx_file")
def validate_xlsx_file(file_path: Path) -> bool:
    """
    Validate that the file is a valid XLSX file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if valid
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if not file_path.suffix == '.xlsx':
        raise ValueError(f"File is not .xlsx: {file_path}")
    
    file_size = file_path.stat().st_size
    if file_size < 1_000_000:
        raise ValueError(f"File is too small ({file_size} bytes). Expected > 1 MB.")
    
    return True
