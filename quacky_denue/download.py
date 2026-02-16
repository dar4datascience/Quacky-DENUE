from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from quacky_denue.models import DownloadLink
from quacky_denue.retry import retry

LOGGER = logging.getLogger(__name__)


def _filename_from_url(url: str) -> str:
    path = urlparse(url).path
    return Path(path).name or "denue_download_csv.zip"


def download_zip(link: DownloadLink, download_dir: Path) -> Path:
    download_dir.mkdir(parents=True, exist_ok=True)
    file_path = download_dir / _filename_from_url(link.href)

    def _do_download() -> Path:
        with urlopen(link.href, timeout=120) as response, file_path.open("wb") as out_file:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out_file.write(chunk)
        return file_path

    local_file = retry(
        operation_name=f"download:{file_path.name}",
        fn=_do_download,
        retries=3,
        base_delay_seconds=2,
        logger=LOGGER,
    )

    LOGGER.info("Downloaded %s -> %s", link.href, local_file)
    return local_file
