from playwright.sync_api import sync_playwright
from pprint import pprint

def scrape_links(url):
    with sync_playwright() as p:
        # Launch the browser
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        # Go to the desired URL
        page.goto(url)

        # Wait for the page to load completely
        page.wait_for_timeout(5000)  # Wait 5 seconds (adjust as needed)

        # Extract the links
        links = page.query_selector_all('a.aLink[href]')
        extracted_links = []
        
        for link in links:
            href = link.get_attribute('href')
            text = link.inner_text().strip()
            extracted_links.append({'href': href, 'text': text})

        # Close the browser
        browser.close()

        return extracted_links

# Replace with your target URL
url = 'https://www.inegi.org.mx/app/descarga/default.html#'
links = scrape_links(url)

# Count the number of links
num_links = len(links)

# Pretty print the number of links and the link details
print(f"Number of extracted links found: {num_links}")
#pprint(links)


def filter_csv_named_files(links):
    # Filter the links where the 'href' ends with '_csv.zip'
    filtered_links = [link for link in links if link['href'].endswith('_csv.zip')]
    return filtered_links


# Filter the links for CSV named files
csv_named_files = filter_csv_named_files(links)

# Count the number of filtered links
num_csv_links = len(csv_named_files)

# Pretty print the number of CSV links and the filtered link details
print(f"Number of CSV named files found: {num_csv_links}")
#pprint(csv_named_files)

def validate_number_of_denue_csvs_found(url, num_csv_links):
    """Use Playwright to navigate to the URL and extract the number of DENUE CSVs found."""
    with sync_playwright() as p:
        # Launch the browser
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        # Go to the desired URL
        page.goto(url)

        # Wait for the page to load completely
        page.wait_for_timeout(5000)  # Wait 5 seconds (adjust as needed)

        # Extract the value from the <span> element
        value = page.inner_text('span#badge_denue').strip()

        # Close the browser
        browser.close()
        
        number_of_csvs = int(value) - 2

        return  number_of_csvs == num_csv_links  # Return the value as an integer
    

csv_links_found_validation = validate_number_of_denue_csvs_found(url, num_csv_links) 

from playwright.sync_api import sync_playwright, TimeoutError
import os
import time

def click_csv_links(url, csv_named_files, download_dir):
    with sync_playwright() as p:
        # Launch the browser
        browser = p.chromium.launch(headless=False)
        
        # Create a new context with the option to accept downloads
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Ensure the download directory exists
        os.makedirs(download_dir, exist_ok=True)

        # Keep track of active downloads
        active_downloads = []

        # Event handler for download events
        def handle_download(download):
            print(f"Download started: {download.suggested_filename}")
            # Save the download to the specified directory
            download_path = os.path.join(download_dir, download.suggested_filename)
            download.save_as(download_path)
            active_downloads.append(download)

        # Attach event handler to the page
        page.on("download", handle_download)

        # Go to the desired URL
        page.goto(url)

        # Wait for the page to load completely
        page.wait_for_timeout(3000)  # Wait 3 seconds initially

        # Build a dictionary of hrefs and their corresponding `a` elements
        a_tags_dict = {}
        links = page.query_selector_all('a[href]')
        for link in links:
            href = link.get_attribute('href')
            a_tags_dict[href] = link

        # Function to handle clicking with retry logic
        def click_with_retry(element, retries=3, delay=5):
            for attempt in range(retries):
                try:
                    time.sleep(delay)
                    element.scroll_into_view_if_needed()  # Scroll the element into view if needed
                    element.click()
                    return True
                except TimeoutError:
                    print(f"Attempt {attempt + 1} failed. Retrying...")
                    time.sleep(delay)
                except Exception as e:
                    print(f"An error occurred: {e}")
                    return False
            return False

        # Click on the links that match the href values in csv_named_files
        for csv_file in csv_named_files:
            href = csv_file['href']
            if href in a_tags_dict:
                print(f"Clicking on: {href}")
                success = click_with_retry(a_tags_dict[href])
                if success:
                    print(f"Successfully clicked on: {href}")
                else:
                    print(f"Failed to click on: {href}")

        # Wait for all downloads to complete
        while active_downloads:
            print(f"Waiting for {len(active_downloads)} downloads to complete...")
            for download in active_downloads:
                download.path()  # This will block until the download is complete
            active_downloads = [download for download in active_downloads if not download.finished()]

        # Close the browser
        browser.close()

# Usage example
download_dir = "denue zips/"
click_csv_links(url, csv_named_files, download_dir)
