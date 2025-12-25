"""High-reliability Google Maps scraping primitives."""

from __future__ import annotations

import contextlib
import time
from typing import List, Optional
from urllib.parse import quote_plus

from playwright.sync_api import Playwright, TimeoutError as PlaywrightTimeoutError, sync_playwright
from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config import (
    CAPTCHA_BACKOFF_S,
    MAX_DELAY_S,
    MIN_DELAY_S,
    PLAYWRIGHT_USER_DATA,
)
from utils import Lead, human_throttle, is_aggregator


class CaptchaDetected(RuntimeError):
    ...


class GoogleMapsScraper:
    """Encapsulates the browser automation for scraping Google Maps listings."""

    def __init__(self, headless: bool = False):
        self.headless = headless
        self._play: Optional[Playwright] = None
        self._browser = None
        self._context = None
        self._page = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if self._page:
            return
        self._play = sync_playwright().start()
        self._browser = self._play.chromium.launch_persistent_context(
            user_data_dir=str(PLAYWRIGHT_USER_DATA),
            headless=self.headless,
            viewport={"width": 1400, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
            locale="en-US",
        )
        self._context = self._browser
        self._page = self._context.pages[0] if self._context.pages else self._context.new_page()

    def stop(self):
        with contextlib.suppress(Exception):
            if self._context:
                self._context.close()
        with contextlib.suppress(Exception):
            if self._play:
                self._play.stop()
        self._play = None
        self._browser = None
        self._context = None
        self._page = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(self, keyword: str, location: str) -> List[Lead]:
        if not self._page:
            raise RuntimeError("Call start() before search().")

        query = f"{keyword} in {location}"
        url = f"https://www.google.com/maps/search/{quote_plus(query)}?hl=en&gl=nl"

        try:
            self._attempt_navigation(url)
        except RetryError as exc:
            raise RuntimeError(f"Navigation failed for query '{query}': {exc}") from exc

        leads: List[Lead] = []
        processed = 0
        stable_rounds = 0

        while True:
            self._check_captcha()

            cards = self._cards_locator()
            count = cards.count()
            if count == 0:
                break

            for i in range(processed, count):
                card = cards.nth(i)
                lead = self._extract_card(card, keyword, location)
                if lead:
                    leads.append(lead)
                human_throttle(MIN_DELAY_S, MAX_DELAY_S)

            if count == processed:
                stable_rounds += 1
            else:
                stable_rounds = 0
            processed = count

            if stable_rounds >= 3:
                break

            self._scroll_results_panel(cards)

        return leads

    # ------------------------------------------------------------------
    # Navigation helpers
    # ------------------------------------------------------------------

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(PlaywrightTimeoutError),
    )
    def _attempt_navigation(self, url: str):
        self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
        self._accept_cookies()
        self._wait_for_feed()

    def _accept_cookies(self):
        selectors = [
            'button:has-text("Accept all")',
            'button:has-text("I agree")',
            'button:has-text("Accept")',
            'button:has-text("Agree")',
            'button[aria-label="Accept all"]',
        ]
        for selector in selectors:
            with contextlib.suppress(Exception):
                btn = self._page.locator(selector)
                if btn.count() > 0 and btn.first.is_visible():
                    btn.first.click()
                    self._page.wait_for_timeout(500)
                    break

    def _wait_for_feed(self):
        self._page.wait_for_selector(
            'div[role="feed"], div[aria-label*="Results"], section[aria-label*="Results"]',
            timeout=40000,
        )

    def _cards_locator(self):
        return self._page.locator('div[role="feed"] div.Nv2PK')

    def _scroll_results_panel(self, cards_locator):
        with contextlib.suppress(Exception):
            cards_locator.first.scroll_into_view_if_needed()
        with contextlib.suppress(Exception):
            self._page.mouse.wheel(0, 1600)
        self._page.wait_for_timeout(1200)

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _extract_card(self, card, keyword: str, location: str) -> Optional[Lead]:
        try:
            name = self._extract_name(card)
            if not name:
                return None

            place_url = card.locator('a[href*="/maps/place/"]').first.get_attribute("href")
            website = self._extract_card_website(card)

            if not website:
                card.click(timeout=6000)
                self._page.wait_for_timeout(1200)
                website = self._extract_sidebar_website()

            if website:
                if is_aggregator(website):
                    website = None

            return Lead(name=name, website=website, location=location, keyword=keyword, place_url=place_url)
        except PlaywrightTimeoutError:
            return None
        except Exception:
            return None

    def _extract_name(self, card) -> Optional[str]:
        with contextlib.suppress(Exception):
            label = card.locator('a[href*="/maps/place/"]').first.get_attribute("aria-label")
            if label:
                return label.strip()
        with contextlib.suppress(Exception):
            text = card.inner_text()
            if text:
                return text.split("\n", 1)[0].strip()
        return None

    def _extract_card_website(self, card) -> Optional[str]:
        selectors = ["a[href^='http']:not([href*='google.'])"]
        for selector in selectors:
            with contextlib.suppress(Exception):
                links = card.locator(selector)
                for i in range(min(5, links.count())):
                    href = links.nth(i).get_attribute("href")
                    if href and href.startswith("http") and "google." not in href:
                        return href
        return None

    def _extract_sidebar_website(self) -> Optional[str]:
        sidebar_selectors = [
            'a:has-text("Website")',
            'a[aria-label*="Website"]',
            'a[data-item-id="authority"]',
            'a[href^="http"]:has-text("Website")',
        ]
        for selector in sidebar_selectors:
            with contextlib.suppress(Exception):
                link = self._page.locator(selector).first
                if link.count() and link.is_visible():
                    href = link.get_attribute("href")
                    if href and href.startswith("http") and "google." not in href:
                        return href
        with contextlib.suppress(Exception):
            main = self._page.locator('div[role="main"]').first
            links = main.locator('a[href^="http"]:not([href*="google"])')
            for i in range(min(5, links.count())):
                href = links.nth(i).get_attribute("href")
                if href and href.startswith("http") and "google." not in href:
                    return href
        return None

    # ------------------------------------------------------------------
    # Safeguards
    # ------------------------------------------------------------------

    def _check_captcha(self):
        try:
            html = self._page.content()
        except Exception:
            return
        markers = [
            "unusual traffic",
            "detected unusual traffic",
            "To continue, please verify",
            "solve the CAPTCHA",
        ]
        if any(marker.lower() in html.lower() for marker in markers):
            time.sleep(CAPTCHA_BACKOFF_S)
            raise CaptchaDetected("Captcha encountered")
