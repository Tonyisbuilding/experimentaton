"""One-time script to commit all buffered leads from split files to Google Sheet."""

import csv
from pathlib import Path
from typing import Set

from config import BATCH_DIR, WEBHOOK_URL, WEBHOOK_SECRET
from webhook import send_batch_to_webhook


def load_buffer_file(filepath: Path) -> list:
    """Load leads from a CSV buffer file."""
    leads = []
    try:
        with open(filepath, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                leads.append(row)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return leads


def main():
    batch_dir = BATCH_DIR
    
    # Find all split buffer files
    buffer_files = list(batch_dir.glob("split_buffer_*.csv"))
    print(f"Found {len(buffer_files)} buffer files in {batch_dir}")
    
    # Load and deduplicate all leads
    all_leads = []
    seen_place_ids: Set[str] = set()
    seen_domains: Set[str] = set()
    
    for filepath in buffer_files:
        leads = load_buffer_file(filepath)
        print(f"  {filepath.name}: {len(leads)} leads")
        for lead in leads:
            # Deduplicate by place_id first, then by domain
            place_id = lead.get('place_id', '')
            if place_id and place_id in seen_place_ids:
                continue
            
            website = lead.get('website', '')
            if website:
                from urllib.parse import urlparse
                try:
                    domain = urlparse(website).netloc.lower()
                    domain = domain.replace('www.', '')
                except:
                    domain = website
                if domain and domain in seen_domains:
                    continue
                if domain:
                    seen_domains.add(domain)
            
            if place_id:
                seen_place_ids.add(place_id)
            all_leads.append(lead)
    
    print(f"\nTotal unique leads: {len(all_leads)}")
    
    if not all_leads:
        print("No leads to commit!")
        return
    
    # Send to webhook in batches
    BATCH_SIZE = 50
    
    class LeadWrapper:
        """Wrapper to make dict compatible with webhook function."""
        def __init__(self, data):
            self.name = data.get('name', '')
            self.website = data.get('website', '')
            self.location = data.get('location', '')
            self.keyword = data.get('keyword', '')
            self.place_url = data.get('place_url', '')
            self.status = ''
    
    total_sent = 0
    for i in range(0, len(all_leads), BATCH_SIZE):
        batch = all_leads[i:i+BATCH_SIZE]
        wrapped_batch = [LeadWrapper(lead) for lead in batch]
        
        batch_num = (i // BATCH_SIZE) + 1
        print(f"\nSending batch {batch_num} ({len(batch)} leads)...")
        
        success = send_batch_to_webhook(batch_num, wrapped_batch)
        if success:
            total_sent += len(batch)
        else:
            print(f"  Failed to send batch {batch_num}!")
    
    print(f"\n{'='*50}")
    print(f"COMPLETE: Sent {total_sent} leads to Google Sheet")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
