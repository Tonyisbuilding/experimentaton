"""Hybrid Scraper: Buffered scroll extraction with adaptive density handling.

This module implements the core scraping logic for Google Maps:
- Buffered scrolling: captures items immediately before Virtual DOM recycles them
- Density detection: triggers SPLIT when unique_count > threshold
- Delayed retry: handles plateaus by waiting and retrying before declaring stuck
- Place ID extraction: deduplicates by unique Google Place ID
"""

from __future__ import annotations

import contextlib
import re
import time
from dataclasses import dataclass, field
from typing import List, Optional, Set, Tuple
from urllib.parse import quote_plus, urlparse, parse_qs

from playwright.sync_api import Playwright, sync_playwright, Page, Locator

from config import (
    CAPTCHA_BACKOFF_S,
    MAX_DELAY_S,
    MIN_DELAY_S,
    PLAYWRIGHT_USER_DATA,
    UNIQUE_COUNT_THRESHOLD,
    PLATEAU_SCROLLS,
    DELAYED_RETRY_ATTEMPTS,
    DELAYED_RETRY_WAIT_SEC,
    MAX_GRID_DEPTH,
)
from utils import human_throttle, is_aggregator


@dataclass
class ScrapedLead:
    """A lead extracted from Google Maps."""
    name: str
    website: Optional[str] = None
    place_url: Optional[str] = None
    place_id: Optional[str] = None  # Extracted from place_url
    city: Optional[str] = None
    street: Optional[str] = None
    location: str = ""  # Search location used
    keyword: str = ""  # Search keyword used


@dataclass
class ScrapeResult:
    """Result of a single job scrape."""
    status: str  # "DONE" or "SPLIT"
    buffer: List[ScrapedLead] = field(default_factory=list)
    unique_count: int = 0
    bbox: Optional[Tuple[float, float, float, float]] = None  # Extracted from map viewport


class CaptchaDetected(Exception):
    """Raised when Google presents a captcha challenge."""
    pass


def extract_place_id(place_url: str) -> Optional[str]:
    """Extract Place ID from Google Maps URL.
    
    Google Maps URLs contain Place ID in various formats:
    - Query param: ?place_id=ChIJ...
    - Data param: /data=...!1s0x...
    - Embedded hex: !1sChIJ...
    
    Returns:
        The Place ID string, or None if not found
    """
    if not place_url:
        return None
    
    # Try query parameter first
    parsed = urlparse(place_url)
    params = parse_qs(parsed.query)
    if "place_id" in params:
        return params["place_id"][0]
    
    # Try to extract from data parameter (more common)
    # Pattern: !1s followed by hex or ChIJ identifier
    match = re.search(r'!1s(0x[a-fA-F0-9]+:[a-fA-F0-9]+|ChIJ[A-Za-z0-9_-]+)', place_url)
    if match:
        return match.group(1)
    
    # Fallback: use the place URL path as a pseudo-ID
    # Extract from /place/NAME/@LAT,LNG format
    path_match = re.search(r'/place/([^/@]+)', place_url)
    if path_match:
        return f"name:{path_match.group(1)}"
    
    return None


class HybridScraper:
    """Google Maps scraper with buffered extraction and density handling."""
    
    END_OF_LIST_TEXTS = [
        "You've reached the end of the list",
        "Je hebt het einde van de lijst bereikt",  # Dutch
        "No results found",
        "Geen resultaten gevonden",  # Dutch
    ]
    
    def __init__(self, headless: bool = False, worker_id: int = 0):
        self.headless = headless
        self.worker_id = worker_id
        self._play: Optional[Playwright] = None
        self._browser = None
        self._context = None
        self._page: Optional[Page] = None
    
    def start(self):
        """Launch browser with per-worker persistent profile."""
        from pathlib import Path
        # Each worker gets its own profile directory to avoid lock conflicts
        profile_dir = PLAYWRIGHT_USER_DATA / f"worker_{self.worker_id}"
        profile_dir.mkdir(parents=True, exist_ok=True)
        
        self._play = sync_playwright().start()
        self._browser = self._play.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=self.headless,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        self._page = self._browser.new_page()
    
    def stop(self):
        """Close browser and cleanup."""
        if self._browser:
            self._browser.close()
        if self._play:
            self._play.stop()
        self._page = None
        self._browser = None
        self._play = None
    
    def scrape_job(self, keyword: str, location: str, bbox: Optional[Tuple] = None) -> ScrapeResult:
        """Execute buffered scroll scrape for a single job.
        
        Args:
            keyword: Search keyword
            location: Location string (for initial search)
            bbox: Optional bounding box (for split jobs)
            
        Returns:
            ScrapeResult with status, buffer, and extracted bbox
        """
        # Navigate to search
        self._navigate_to_search(keyword, location, bbox)
        self._accept_cookies()
        
        # Check for captcha
        self._check_captcha()
        
        # Wait for results feed
        if not self._wait_for_feed():
            # No results - return empty DONE
            return ScrapeResult(status="DONE", buffer=[], unique_count=0)
        
        # Extract current viewport bbox for potential splitting
        current_bbox = self._extract_viewport_bbox()
        
        # Buffered scroll loop
        buffer: List[ScrapedLead] = []
        seen_place_ids: Set[str] = set()
        previous_counts: List[int] = []
        
        while True:
            # Scrape visible cards IMMEDIATELY (before Virtual DOM recycles)
            visible_leads = self._scrape_visible_cards(keyword, location)
            
            for lead in visible_leads:
                place_id = lead.place_id or extract_place_id(lead.place_url)
                if place_id and place_id not in seen_place_ids:
                    seen_place_ids.add(place_id)
                    lead.place_id = place_id
                    buffer.append(lead)
            
            unique_count = len(seen_place_ids)
            
            # Assessment 1: Density threshold exceeded
            if unique_count > UNIQUE_COUNT_THRESHOLD:
                print(f"  → Density threshold exceeded ({unique_count} > {UNIQUE_COUNT_THRESHOLD})")
                return ScrapeResult(
                    status="SPLIT",
                    buffer=buffer,
                    unique_count=unique_count,
                    bbox=current_bbox,
                )
            
            # Assessment 2: End of list message
            if self._check_end_of_list():
                print(f"  → End of list reached ({unique_count} unique leads)")
                return ScrapeResult(
                    status="DONE",
                    buffer=buffer,
                    unique_count=unique_count,
                    bbox=current_bbox,
                )
            
            # Assessment 3: Plateau detection
            previous_counts.append(unique_count)
            if len(previous_counts) > PLATEAU_SCROLLS:
                recent = previous_counts[-PLATEAU_SCROLLS:]
                if len(set(recent)) == 1:  # All same count
                    # Plateau detected - try delayed retry
                    print(f"  → Plateau detected at {unique_count}, attempting delayed retry...")
                    if not self._delayed_retry(seen_place_ids, buffer, keyword, location):
                        # Still stuck after retries - treat as SPLIT
                        print(f"  → Still stuck after retries, triggering SPLIT")
                        return ScrapeResult(
                            status="SPLIT",
                            buffer=buffer,
                            unique_count=len(seen_place_ids),
                            bbox=current_bbox,
                        )
                    # Retry succeeded, reset plateau counter
                    previous_counts.clear()
            
            # Scroll down
            self._scroll_results_panel()
            human_throttle(MIN_DELAY_S, MAX_DELAY_S)
    
    def _navigate_to_search(self, keyword: str, location: str, bbox: Optional[Tuple] = None):
        """Navigate to Google Maps search."""
        if bbox:
            # Use bbox center for navigation
            n, e, s, w = bbox
            center_lat = (n + s) / 2
            center_lng = (e + w) / 2
            lat_span = abs(n - s)
            zoom = max(10, min(18, int(16 - lat_span * 50)))
            encoded = quote_plus(keyword)
            url = f"https://www.google.com/maps/search/{encoded}/@{center_lat},{center_lng},{zoom}z"
        else:
            # Use location string
            query = f"{keyword} near {location}"
            encoded = quote_plus(query)
            url = f"https://www.google.com/maps/search/{encoded}"
        
        print(f"  Navigating to: {keyword} @ {location or 'bbox'}")
        self._page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)  # Let map settle
    
    def _accept_cookies(self):
        """Accept cookie consent if present."""
        try:
            accept_btn = self._page.locator("button:has-text('Accept all'), button:has-text('Alles accepteren')")
            if accept_btn.count() > 0:
                accept_btn.first.click()
                time.sleep(1)
        except Exception:
            pass
    
    def _check_captcha(self):
        """Check for captcha and raise if detected."""
        captcha_indicators = [
            "g-recaptcha",
            "captcha",
            "unusual traffic",
        ]
        page_content = self._page.content().lower()
        for indicator in captcha_indicators:
            if indicator in page_content:
                print(f"CAPTCHA detected! Backing off for {CAPTCHA_BACKOFF_S}s...")
                time.sleep(CAPTCHA_BACKOFF_S)
                raise CaptchaDetected("Google captcha challenge detected")
    
    def _wait_for_feed(self, timeout: int = 10) -> bool:
        """Wait for results feed to load. Returns False if no results."""
        try:
            # Wait for either results or "no results" message
            self._page.wait_for_selector(
                "div[role='feed'], div:has-text('No results'), div:has-text('Geen resultaten')",
                timeout=timeout * 1000
            )
            # Check if we got actual results
            feed = self._page.locator("div[role='feed']")
            return feed.count() > 0
        except Exception:
            return False
    
    def _extract_viewport_bbox(self) -> Optional[Tuple[float, float, float, float]]:
        """Extract current map viewport bounding box from URL or map state."""
        try:
            url = self._page.url
            # Parse @lat,lng,zoom from URL
            match = re.search(r'@(-?\d+\.?\d*),(-?\d+\.?\d*),(\d+\.?\d*)z', url)
            if match:
                lat, lng, zoom = float(match.group(1)), float(match.group(2)), float(match.group(3))
                # Estimate bbox from zoom level
                # Rough approximation: each zoom level halves the view
                span = 360 / (2 ** zoom)
                return (
                    lat + span / 2,  # north
                    lng + span / 2,  # east
                    lat - span / 2,  # south
                    lng - span / 2,  # west
                )
        except Exception as e:
            print(f"  Warning: Could not extract viewport bbox: {e}")
        return None
    
    def _scrape_visible_cards(self, keyword: str, location: str) -> List[ScrapedLead]:
        """Scrape all currently visible result cards."""
        leads = []
        cards = self._page.locator("div[role='feed'] > div > div[jsaction]")
        
        for i in range(cards.count()):
            try:
                card = cards.nth(i)
                if not card.is_visible():
                    continue
                
                lead = self._extract_card_data(card, keyword, location)
                if lead and lead.name:
                    leads.append(lead)
            except Exception:
                continue
        
        return leads
    
    def _extract_card_data(self, card: Locator, keyword: str, location: str) -> Optional[ScrapedLead]:
        """Extract data from a single result card."""
        try:
            # Get name from the card title
            name_el = card.locator("div.fontHeadlineSmall, a.fontHeadlineSmall")
            name = name_el.first.inner_text() if name_el.count() > 0 else ""
            
            if not name:
                return None
            
            # Get place URL from the card link
            place_url = None
            link = card.locator("a[href*='/maps/place/']")
            if link.count() > 0:
                place_url = link.first.get_attribute("href")
            
            # Extract place ID
            place_id = extract_place_id(place_url) if place_url else None
            
            # Try to get website (may need to click into detail)
            website = self._try_extract_website(card)
            
            return ScrapedLead(
                name=name.strip(),
                website=website,
                place_url=place_url,
                place_id=place_id,
                keyword=keyword,
                location=location,
            )
        except Exception:
            return None
    
    def _try_extract_website(self, card: Locator) -> Optional[str]:
        """Try to extract website from card without clicking into detail."""
        try:
            # Look for website button/link in the card
            website_btn = card.locator("a[data-value='Website'], a[aria-label*='website'], button[aria-label*='website']")
            if website_btn.count() > 0:
                href = website_btn.first.get_attribute("href")
                if href and not href.startswith("javascript:"):
                    # Filter aggregators
                    if not is_aggregator(href):
                        return href
        except Exception:
            pass
        return None
    
    def _check_end_of_list(self) -> bool:
        """Check if 'end of list' message is visible."""
        try:
            for text in self.END_OF_LIST_TEXTS:
                end_msg = self._page.locator(f"div:has-text('{text}')")
                if end_msg.count() > 0:
                    return True
        except Exception:
            pass
        return False
    
    def _scroll_results_panel(self):
        """Scroll the results panel down."""
        try:
            feed = self._page.locator("div[role='feed']")
            if feed.count() > 0:
                feed.evaluate("el => el.scrollTop = el.scrollHeight")
        except Exception:
            pass
    
    def _delayed_retry(
        self,
        seen_place_ids: Set[str],
        buffer: List[ScrapedLead],
        keyword: str,
        location: str
    ) -> bool:
        """Attempt delayed retries when plateau detected.
        
        Returns:
            True if count increased (not stuck), False if still stuck
        """
        initial_count = len(seen_place_ids)
        
        for attempt in range(DELAYED_RETRY_ATTEMPTS):
            print(f"    Retry {attempt + 1}/{DELAYED_RETRY_ATTEMPTS}...")
            time.sleep(DELAYED_RETRY_WAIT_SEC)
            self._scroll_results_panel()
            time.sleep(1)
            
            # Scrape again
            visible_leads = self._scrape_visible_cards(keyword, location)
            for lead in visible_leads:
                place_id = lead.place_id or extract_place_id(lead.place_url)
                if place_id and place_id not in seen_place_ids:
                    seen_place_ids.add(place_id)
                    lead.place_id = place_id
                    buffer.append(lead)
            
            if len(seen_place_ids) > initial_count:
                print(f"    Count increased to {len(seen_place_ids)}, continuing...")
                return True
        
        return False


# Context manager for clean usage
@contextlib.contextmanager
def create_scraper(headless: bool = False):
    """Context manager for HybridScraper lifecycle."""
    scraper = HybridScraper(headless=headless)
    scraper.start()
    try:
        yield scraper
    finally:
        scraper.stop()
