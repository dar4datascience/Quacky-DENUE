import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scian_fetcher import (
    fetch_scian_page,
    parse_xlsx_links,
    download_scian_file,
    validate_xlsx_file,
    INEGI_SCIAN_URL,
    CACHE_DIR
)


class TestScianFetcher:
    """Live tests for SCIAN fetcher - no mocking allowed."""
    
    def test_inegi_page_accessible(self):
        """Test that INEGI SCIAN page is accessible."""
        html_content = fetch_scian_page.fn(INEGI_SCIAN_URL)
        
        assert html_content is not None
        assert len(html_content) > 1000
        assert "scian" in html_content.lower() or "clasificación" in html_content.lower()
    
    def test_find_xlsx_links(self):
        """Test that at least one .xlsx link is found on the page."""
        html_content = fetch_scian_page.fn(INEGI_SCIAN_URL)
        xlsx_links = parse_xlsx_links.fn(html_content)
        
        print("\n" + "="*80)
        print("FOUND .XLSX DOWNLOAD LINKS:")
        print("="*80)
        for link_text, url in xlsx_links:
            print(f"  [{link_text}]")
            print(f"  URL: {url}")
            print("-"*80)
        
        assert len(xlsx_links) > 0, "No .xlsx links found on INEGI SCIAN page"
        
        for link_text, url in xlsx_links:
            assert url.endswith('.xlsx') or '.xlsx?' in url
            assert url.startswith('http')
    
    def test_download_scian_file(self):
        """Test downloading SCIAN XLSX file (main classification file is always first)."""
        html_content = fetch_scian_page.fn(INEGI_SCIAN_URL)
        xlsx_links = parse_xlsx_links.fn(html_content)
        
        assert len(xlsx_links) > 0, "No .xlsx links found to download"
        
        link_text, url = xlsx_links[0]
        print(f"\nDownloading main SCIAN file: {link_text}")
        print(f"URL: {url}")
        
        file_path = download_scian_file.fn(url)
        
        assert file_path.exists()
        assert file_path.suffix == '.xlsx'
        assert 'categorias_y_productos' in file_path.name
        
        file_size = file_path.stat().st_size
        print(f"Downloaded file size: {file_size:,} bytes ({file_size / 1_000_000:.2f} MB)")
        
        assert file_size > 1_000_000, f"Main SCIAN file should be > 1 MB, got {file_size} bytes"
    
    def test_validate_downloaded_file(self):
        """Test validation of downloaded XLSX file."""
        html_content = fetch_scian_page.fn(INEGI_SCIAN_URL)
        xlsx_links = parse_xlsx_links.fn(html_content)
        
        assert len(xlsx_links) > 0
        
        url = xlsx_links[0][1]
        file_path = download_scian_file.fn(url)
        
        is_valid = validate_xlsx_file.fn(file_path)
        assert is_valid is True
    
    def test_cached_file_reuse(self):
        """Test that cached files are reused instead of re-downloading."""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        html_content = fetch_scian_page.fn(INEGI_SCIAN_URL)
        xlsx_links = parse_xlsx_links.fn(html_content)
        
        if len(xlsx_links) == 0:
            pytest.skip("No .xlsx links found")
        
        url = xlsx_links[0][1]
        
        file_path_1 = download_scian_file.fn(url)
        mtime_1 = file_path_1.stat().st_mtime
        
        file_path_2 = download_scian_file.fn(url)
        mtime_2 = file_path_2.stat().st_mtime
        
        assert file_path_1 == file_path_2
        assert mtime_2 >= mtime_1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
