import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from playwright.sync_api import sync_playwright

def debug_fetch():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print("Navigating to INEGI page...")
        page.goto("https://www.inegi.org.mx/app/descarga/?ti=6")
        
        print("Waiting for page to load...")
        page.wait_for_timeout(5000)
        
        print("\nChecking for #denue tab...")
        denue_tab = page.query_selector('#denue')
        print(f"#denue tab found: {denue_tab is not None}")
        
        if denue_tab:
            print(f"#denue is visible: {denue_tab.is_visible()}")
        
        print("\nLooking for data rows...")
        rows = page.query_selector_all('tr[data-nivel="3"][data-agrupacion="denue"]')
        print(f"Found {len(rows)} rows with data-nivel='3' and data-agrupacion='denue'")
        
        print("\nTrying alternative selectors...")
        all_trs = page.query_selector_all('tr')
        print(f"Total <tr> elements: {len(all_trs)}")
        
        denue_rows = page.query_selector_all('tr[data-agrupacion="denue"]')
        print(f"Rows with data-agrupacion='denue': {len(denue_rows)}")
        
        if len(denue_rows) > 0:
            print("\nSample row attributes:")
            sample = denue_rows[0]
            print(f"  data-nivel: {sample.get_attribute('data-nivel')}")
            print(f"  data-agrupacion: {sample.get_attribute('data-agrupacion')}")
            print(f"  data-titulo: {sample.get_attribute('data-titulo')}")
        
        print("\nPage title:", page.title())
        print("Current URL:", page.url)
        
        input("\nPress Enter to close browser...")
        browser.close()

if __name__ == '__main__':
    debug_fetch()
