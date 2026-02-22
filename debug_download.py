import sys
from pathlib import Path
import tempfile
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from denue_downloader import DENUEDownloader
import uuid

temp_dir = tempfile.mkdtemp(prefix="debug_download_")
print(f"Using temp dir: {temp_dir}")

downloader = DENUEDownloader(cache_dir=temp_dir, max_retries=2, timeout=5)

unique_id = str(uuid.uuid4())
print(f"Unique ID: {unique_id}")

zip_path = downloader.download_dataset(
    "https://www.inegi.org.mx/invalid_url_that_does_not_exist.zip",
    f"Invalid Test Sector {unique_id}",
    f"99/9999"
)

print(f"\nResult: {zip_path}")
print(f"Is None: {zip_path is None}")

if zip_path and zip_path.exists():
    print(f"File exists: {zip_path}")
    print(f"File size: {zip_path.stat().st_size} bytes")
    
print(f"\nFiles in cache dir:")
for f in Path(temp_dir).glob("*"):
    print(f"  {f.name}: {f.stat().st_size} bytes")
