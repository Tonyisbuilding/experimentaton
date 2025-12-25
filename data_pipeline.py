"""Data Pipeline: Deduplication and transactional commits.

Handles:
- Composite deduplication (Place ID → Domain+City → Name+Street+City)
- Transient batch files for crash safety
- Webhook commits to Google Sheets
- Union strategy for SPLIT jobs
"""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import List, Optional, Set
from urllib.parse import urlparse

from hybrid_scraper import ScrapedLead
from webhook import send_batch_to_webhook


def extract_domain(url: Optional[str]) -> Optional[str]:
    """Extract domain from URL."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


def get_dedup_key(lead: ScrapedLead) -> str:
    """Generate composite deduplication key.
    
    Priority:
    1. Place ID (most reliable)
    2. Domain + City (for businesses with websites)
    3. Name + Street + City (fallback)
    """
    # Primary: Place ID
    if lead.place_id:
        return f"pid:{lead.place_id}"
    
    # Secondary: Domain + City
    domain = extract_domain(lead.website)
    city = (lead.city or "").strip().lower()
    if domain and city:
        return f"dom:{domain}_{city}"
    
    # Fallback: Name + Street + City
    name = (lead.name or "").strip().lower()
    street = (lead.street or "").strip().lower()
    if name:
        return f"name:{name}_{street}_{city}"
    
    # Last resort: just the name
    return f"raw:{lead.name or 'unknown'}"


def dedupe_local(leads: List[ScrapedLead]) -> List[ScrapedLead]:
    """Remove duplicates within a buffer."""
    seen = set()
    unique = []
    for lead in leads:
        key = get_dedup_key(lead)
        if key not in seen:
            seen.add(key)
            unique.append(lead)
    return unique


def dedupe_global(
    leads: List[ScrapedLead],
    global_seen: Set[str]
) -> List[ScrapedLead]:
    """Filter leads against global history."""
    return [lead for lead in leads if get_dedup_key(lead) not in global_seen]


def write_batch_csv(filepath: Path, leads: List[ScrapedLead]):
    """Write leads to CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = ["name", "website", "place_url", "place_id", "location", "keyword", "city", "street"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lead in leads:
            writer.writerow({
                "name": lead.name,
                "website": lead.website or "",
                "place_url": lead.place_url or "",
                "place_id": lead.place_id or "",
                "location": lead.location,
                "keyword": lead.keyword,
                "city": lead.city or "",
                "street": lead.street or "",
            })


def read_batch_csv(filepath: Path) -> List[ScrapedLead]:
    """Read leads from CSV file."""
    if not filepath.exists():
        return []
    
    leads = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            leads.append(ScrapedLead(
                name=row.get("name", ""),
                website=row.get("website") or None,
                place_url=row.get("place_url") or None,
                place_id=row.get("place_id") or None,
                location=row.get("location", ""),
                keyword=row.get("keyword", ""),
                city=row.get("city") or None,
                street=row.get("street") or None,
            ))
    return leads


class DataPipeline:
    """Manages the commit pipeline with crash safety.
    
    Traffic Light Strategy:
    - Green Lane: DONE jobs → commit immediately to Master CSV + webhook
    - Yellow Lane: SPLIT jobs → persist to staging dir for later union
    """
    
    def __init__(self, batch_dir: Path, global_seen: Set[str]):
        from config import MASTER_CSV, STAGING_DIR
        
        self.batch_dir = batch_dir
        self.batch_dir.mkdir(parents=True, exist_ok=True)
        
        self.master_csv = MASTER_CSV
        self.staging_dir = STAGING_DIR
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        self.global_seen = global_seen
        self.batch_counter = 0
    
    def commit_job_to_master(
        self,
        job_id: str,
        buffer: List[ScrapedLead],
    ) -> List[ScrapedLead]:
        """Green Lane: Commit job buffer directly to Master CSV + webhook.
        
        Args:
            job_id: The job ID for file naming
            buffer: List of scraped leads
            
        Returns:
            List of new leads that were committed
        """
        # 1. Local dedup
        local_unique = dedupe_local(buffer)
        print(f"  Local dedup: {len(buffer)} → {len(local_unique)}")
        
        # 2. Global dedup
        new_leads = dedupe_global(local_unique, self.global_seen)
        print(f"  Global dedup: {len(local_unique)} → {len(new_leads)}")
        
        if not new_leads:
            print("  No new leads to commit")
            return []
        
        # 3. Append to Master CSV
        self._append_to_master_csv(new_leads)
        print(f"  Appended {len(new_leads)} to Master CSV")
        
        # 4. Push to Google Sheets via webhook
        self.batch_counter += 1
        try:
            from utils import Lead
            webhook_leads = [
                Lead(
                    name=lead.name,
                    website=lead.website or "",
                    location=lead.location,
                    keyword=lead.keyword,
                    place_url=lead.place_url or "",
                )
                for lead in new_leads
            ]
            send_batch_to_webhook(self.batch_counter, webhook_leads)
            print(f"  🟢 Webhook push successful (batch {self.batch_counter})")
        except Exception as e:
            print(f"  Warning: Webhook push failed: {e}")
        
        # 5. Update global seen
        for lead in new_leads:
            self.global_seen.add(get_dedup_key(lead))
        
        return new_leads
    
    def _append_to_master_csv(self, leads: List[ScrapedLead]):
        """Append leads to Master CSV file."""
        fieldnames = ["name", "website", "place_url", "place_id", "location", "keyword", "city", "street"]
        
        write_header = not self.master_csv.exists() or self.master_csv.stat().st_size == 0
        self.master_csv.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.master_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            for lead in leads:
                writer.writerow({
                    "name": lead.name,
                    "website": lead.website or "",
                    "place_url": lead.place_url or "",
                    "place_id": lead.place_id or "",
                    "location": lead.location,
                    "keyword": lead.keyword,
                    "city": lead.city or "",
                    "street": lead.street or "",
                })


    def commit_job(
        self,
        job_id: str,
        buffer: List[ScrapedLead],
        on_success: callable = None,
    ) -> List[ScrapedLead]:
        """Commit job buffer through the pipeline.
        
        Args:
            job_id: The job ID for file naming
            buffer: List of scraped leads
            on_success: Callback when commit succeeds
            
        Returns:
            List of new leads that were committed
        """
        # 1. Local dedup
        local_unique = dedupe_local(buffer)
        print(f"  Local dedup: {len(buffer)} → {len(local_unique)}")
        
        # 2. Global dedup
        new_leads = dedupe_global(local_unique, self.global_seen)
        print(f"  Global dedup: {len(local_unique)} → {len(new_leads)}")
        
        if not new_leads:
            print("  No new leads to commit")
            return []
        
        # 3. Transient batch save (crash safety)
        batch_file = self.batch_dir / f"leads_batch_{job_id}.csv"
        write_batch_csv(batch_file, new_leads)
        print(f"  Saved transient batch: {batch_file}")
        
        # 4. Push to Google Sheets via webhook
        self.batch_counter += 1
        # Convert to dict format for webhook
        leads_dicts = [
            {
                "name": lead.name,
                "website": lead.website,
                "location": lead.location,
                "keyword": lead.keyword,
                "place_url": lead.place_url,
            }
            for lead in new_leads
        ]
        
        try:
            from utils import Lead
            webhook_leads = [
                Lead(
                    name=lead.name,
                    website=lead.website or "",
                    location=lead.location,
                    keyword=lead.keyword,
                    place_url=lead.place_url or "",
                )
                for lead in new_leads
            ]
            send_batch_to_webhook(self.batch_counter, webhook_leads)
            print(f"  Webhook push successful (batch {self.batch_counter})")
        except Exception as e:
            print(f"  Warning: Webhook push failed: {e}")
            # Don't fail - the batch file is our safety net
        
        # 5. Cleanup and update global history
        try:
            os.remove(batch_file)
            print(f"  Cleaned up transient file")
        except Exception:
            pass
        
        # Update global seen
        for lead in new_leads:
            self.global_seen.add(get_dedup_key(lead))
        
        if on_success:
            on_success()
        
        return new_leads
    
    def persist_split_buffer(self, job_id: str, buffer: List[ScrapedLead]) -> str:
        """Save buffer for a SPLIT job (to be unioned later).
        
        Returns:
            Path to the persisted buffer file
        """
        buffer_file = self.batch_dir / f"split_buffer_{job_id}.csv"
        write_batch_csv(buffer_file, buffer)
        print(f"  Persisted SPLIT buffer: {buffer_file}")
        return str(buffer_file)
    
    def union_and_commit(
        self,
        parent_id: str,
        parent_buffer_file: Optional[str],
        children_buffer_files: List[str],
        on_success: callable = None,
    ) -> List[ScrapedLead]:
        """Union parent + children buffers and commit.
        
        This is called when all children of a SPLIT job are done.
        """
        all_leads = []
        
        # Load parent buffer
        if parent_buffer_file and Path(parent_buffer_file).exists():
            all_leads.extend(read_batch_csv(Path(parent_buffer_file)))
        
        # Load children buffers
        for child_file in children_buffer_files:
            if child_file and Path(child_file).exists():
                all_leads.extend(read_batch_csv(Path(child_file)))
        
        print(f"  Union: {len(all_leads)} total leads from parent + children")
        
        # Commit the union
        result = self.commit_job(f"union_{parent_id}", all_leads, on_success)
        
        # Cleanup buffer files
        for filepath in [parent_buffer_file] + children_buffer_files:
            if filepath:
                try:
                    os.remove(filepath)
                except Exception:
                    pass
        
        return result
    
    def recover_pending_batches(self) -> int:
        """Recover any pending batch files from previous crash.
        
        Returns:
            Number of batches recovered
        """
        recovered = 0
        for batch_file in self.batch_dir.glob("leads_batch_*.csv"):
            leads = read_batch_csv(batch_file)
            if leads:
                print(f"Recovering batch: {batch_file.name} ({len(leads)} leads)")
                # These were written but webhook may have failed
                # Try to push them again
                try:
                    from utils import Lead
                    webhook_leads = [
                        Lead(
                            name=lead.name,
                            website=lead.website or "",
                            location=lead.location,
                            keyword=lead.keyword,
                            place_url=lead.place_url or "",
                        )
                        for lead in leads
                    ]
                    self.batch_counter += 1
                    send_batch_to_webhook(self.batch_counter, webhook_leads)
                    os.remove(batch_file)
                    recovered += 1
                    
                    # Update global seen
                    for lead in leads:
                        self.global_seen.add(get_dedup_key(lead))
                except Exception as e:
                    print(f"  Recovery failed for {batch_file.name}: {e}")
        
        return recovered
