from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

from playwright.sync_api import TimeoutError, sync_playwright

from quacky_denue.config import PipelineConfig
from quacky_denue.models import DownloadLink
from quacky_denue.retry import retry

LOGGER = logging.getLogger(__name__)
FEDERATION_PATTERN = re.compile(r"denue_([0-9]{1,2}(?:-[0-9]{1,2})?)_", re.IGNORECASE)
DENUE_CSV_ZIP_PATTERN = re.compile(
    r"/contenidos/masiva/denue/[0-9]{4}/denue_[0-9]{2}(?:-[0-9]{2})?_[0-9]{8}(?:_csv|_shp)\.zip$",
    re.IGNORECASE,
)


def is_denue_csv_zip_url(url: str) -> bool:
    parsed = urlparse(url)
    return bool(DENUE_CSV_ZIP_PATTERN.search(parsed.path)) and parsed.path.lower().endswith("_csv.zip")


def _parse_federation(href: str, text: str) -> str:
    parsed = urlparse(href)
    match = FEDERATION_PATTERN.search(href)
    if not match:
        match = FEDERATION_PATTERN.search(parsed.path)
    if match:
        fed = match.group(1)
        if len(fed) == 1:
            return fed.zfill(2)
        return fed
    return text.strip() or "unknown"


def _perform_optional_login(page, config: PipelineConfig) -> None:
    if not config.login or not config.login.username or not config.login.password:
        return

    login = config.login

    def _login_once() -> None:
        username_input = page.locator(login.username_selector).first
        password_input = page.locator(login.password_selector).first
        submit_btn = page.locator(login.submit_selector).first

        if username_input.count() == 0 or password_input.count() == 0 or submit_btn.count() == 0:
            LOGGER.info("Login fields not found on page, skipping login")
            return

        username_input.fill(login.username)
        password_input.fill(login.password)
        submit_btn.click(timeout=20_000)
        page.wait_for_load_state("networkidle", timeout=30_000)

    retry("login", _login_once, retries=3, base_delay_seconds=2.0, logger=LOGGER)


def discover_denue_links(config: PipelineConfig) -> list[DownloadLink]:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=config.headless)
        context = browser.new_context()
        page = context.new_page()

        page.goto(config.download_url, timeout=60_000)
        page.wait_for_timeout(3_000)
        _perform_optional_login(page, config)

        anchors = page.query_selector_all("a.aLink[href], a[href]")
        links: list[DownloadLink] = []

        for anchor in anchors:
            href = anchor.get_attribute("href")
            if not href:
                continue

            text = anchor.inner_text().strip()
            absolute_href = urljoin(config.download_url, href)
            if not is_denue_csv_zip_url(absolute_href):
                continue

            federation = _parse_federation(absolute_href, text)
            links.append(DownloadLink(href=absolute_href, text=text, federation=federation))

        browser.close()

    unique_links: dict[str, DownloadLink] = {item.href: item for item in links}
    deduped = list(unique_links.values())

    if config.federation_filter:
        filtered = [x for x in deduped if x.federation in config.federation_filter]
    else:
        filtered = deduped

    if config.max_files is not None:
        filtered = filtered[: config.max_files]

    LOGGER.info("Discovered %s candidate DENUE zip links", len(filtered))
    return filtered


def validate_link_count(config: PipelineConfig, discovered_count: int) -> bool:
    """Validate count against badge_denue when available."""
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=config.headless)
            page = browser.new_page()
            page.goto(config.download_url, timeout=60_000)
            page.wait_for_timeout(2_000)
            badge_value = page.inner_text("span#badge_denue").strip()
            browser.close()

        expected = max(int(badge_value) - 2, 0)
        return expected == discovered_count
    except (TimeoutError, ValueError):
        LOGGER.warning("Could not validate discovered links with badge_denue")
        return True
