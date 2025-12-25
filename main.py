"""Batch-orchestrated Google Maps lead scraper."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

from config import BATCH_SIZE, KEYWORDS, LOCATIONS, MASTER_OUTPUT, OUTPUT_DIR
from google_maps_scraper import CaptchaDetected, GoogleMapsScraper
from utils import Lead, append_to_master, dedupe_leads, export_batch
from webhook import send_batch_to_webhook
from common.validator import get_website_status


def prompt_to_continue(batch_number: int) -> bool:
    print(f"\nBatch {batch_number} ready. Press Enter for the next {BATCH_SIZE} leads or type 'q' to stop:", end=" ")
    try:
        response = input().strip().lower()
    except EOFError:
        return False
    return response not in {"q", "quit", "exit"}


def summarise_batch(batch_number: int, leads: List[Lead]):
    print(f"\n--- Batch {batch_number} ({len(leads)} leads) ---")
    for lead in leads:
        website = lead.website or "<no website>"
        print(f" • {lead.name}  |  {website}")


def run(headless: bool = False):
    scraper = GoogleMapsScraper(headless=headless)
    scraper.start()

    batch: List[Lead] = []
    batch_number = 0
    seen_domains = set()
    seen_names = set()
    master_path = MASTER_OUTPUT
    if master_path.exists():
        master_path.unlink()

    try:
        for location in LOCATIONS:
            for keyword in KEYWORDS:
                print(f"\n=== Searching '{keyword}' in '{location}' ===")
                try:
                    leads = scraper.search(keyword, location)
                except CaptchaDetected:
                    print("Captcha encountered. Aborting to protect the session.")
                    return
                except Exception as exc:
                    print(f"Error during search for '{keyword}' -> '{location}': {exc}")
                    continue


                for lead in dedupe_leads(leads, seen_domains, seen_names):
                    # === PHASE 1 VALIDATION ===
                    # 1. Run the check
                    validation_status = get_website_status(lead.website)
                    
                    # 2. Create a new Lead object with the status attached
                    validated_lead = Lead(
                        name=lead.name,
                        website=lead.website,
                        location=lead.location,
                        keyword=lead.keyword,
                        place_url=lead.place_url,
                        status=validation_status
                    )
                    
                    batch.append(validated_lead)
                    if len(batch) == BATCH_SIZE:
                        batch_number += 1
                        export_current_batch(batch, batch_number, master_path)
                        if not prompt_to_continue(batch_number):
                            print("Stopping as requested.")
                            return
                        batch = []

        if batch:
            batch_number += 1
            export_current_batch(batch, batch_number, master_path)
    finally:
        scraper.stop()


def export_current_batch(batch: List[Lead], batch_number: int, master_path: Path):
    destination = OUTPUT_DIR / f"leads_batch_{batch_number:03d}.csv"
    export_batch(batch, destination)
    append_to_master(master_path, batch)
    send_batch_to_webhook(batch_number, batch)
    summarise_batch(batch_number, batch)
    print(f"Saved batch {batch_number} → {destination}")
    print(f"Updated consolidated dataset → {master_path}")


if __name__ == "__main__":
    headless_arg = "--headless" in sys.argv
    parallel_arg = "--parallel" in sys.argv
    
    if parallel_arg:
        from parallel_scraper import run_parallel
        run_parallel(headless=headless_arg)
    else:
        run(headless=headless_arg)
