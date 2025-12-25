"""Parallel orchestrator for Google Maps lead scraping with multiple browser instances.

Uses multiprocessing to avoid Playwright sync/async conflicts.
"""

from __future__ import annotations

import multiprocessing as mp
import random
import sys
import time
import queue
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Set, Tuple, Optional, Dict
from multiprocessing import Process, Queue, Manager

from config import (
    BATCH_SIZE,
    INTER_REQUEST_DELAY_MIN,
    INTER_REQUEST_DELAY_MAX,
    KEYWORDS,
    LOCATIONS,
    MASTER_OUTPUT,
    NUM_WORKERS,
    OUTPUT_DIR,
    WORKER_STAGGER_DELAY,
)


@dataclass
class WorkerResult:
    """Result from a single worker's search operation."""
    worker_id: int
    keyword: str
    location: str
    leads: List[dict] = field(default_factory=list)  # Dicts for multiprocessing serialization
    error: Optional[str] = None
    captcha_detected: bool = False


def worker_process(
    worker_id: int,
    task_queue: Queue,
    result_queue: Queue,
    headless: bool,
    delay_min: int,
    delay_max: int
):
    """
    Worker process that runs its own browser instance.
    Each worker has its own Playwright context to avoid sync/async conflicts.
    """
    # Import inside worker to avoid multiprocessing issues
    from google_maps_scraper import CaptchaDetected, GoogleMapsScraper
    
    print(f"   [W{worker_id + 1}] Starting browser...")
    scraper = GoogleMapsScraper(headless=headless)
    scraper.start()
    print(f"   [W{worker_id + 1}] ✅ Browser ready")
    
    try:
        while True:
            try:
                # Get task from queue (non-blocking with timeout)
                task = task_queue.get(timeout=5)
                
                if task is None:  # Poison pill to stop worker
                    break
                
                keyword, location = task
                result = WorkerResult(worker_id=worker_id, keyword=keyword, location=location)
                
                try:
                    print(f"   [W{worker_id + 1}] Searching '{keyword}' in '{location}'...")
                    leads = scraper.search(keyword, location)
                    
                    # Convert Lead objects to dicts for serialization
                    result.leads = [
                        {
                            'name': l.name,
                            'website': l.website,
                            'location': l.location,
                            'keyword': l.keyword,
                            'place_url': l.place_url,
                        }
                        for l in leads
                    ]
                    
                    # Randomized delay between requests
                    delay = random.uniform(delay_min, delay_max)
                    print(f"   [W{worker_id + 1}] Found {len(leads)} leads. Waiting {delay:.1f}s...")
                    time.sleep(delay)
                    
                except CaptchaDetected:
                    print(f"   [W{worker_id + 1}] ⚠️ CAPTCHA detected!")
                    result.captcha_detected = True
                    result.error = "CAPTCHA detected"
                    
                except Exception as e:
                    print(f"   [W{worker_id + 1}] ❌ Error: {e}")
                    result.error = str(e)
                
                result_queue.put(result)
                
            except queue.Empty:
                # Check if we should continue waiting
                continue
                
    finally:
        print(f"   [W{worker_id + 1}] Stopping browser...")
        scraper.stop()


class ParallelOrchestrator:
    """Coordinates multiple browser processes for parallel scraping."""
    
    def __init__(self, headless: bool = False, num_workers: int = NUM_WORKERS):
        self.headless = headless
        self.num_workers = num_workers
        self.workers: List[Process] = []
        self.task_queue: Optional[Queue] = None
        self.result_queue: Optional[Queue] = None
        
        # Deduplication (main process only)
        self._seen_domains: Set[str] = set()
        self._seen_names: Set[str] = set()
        
        # Results aggregation
        self.batch_number = 0
        
    def start_workers(self):
        """Initialize worker processes with staggered starts."""
        print(f"\n🚀 Starting {self.num_workers} parallel worker processes...")
        
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        
        for i in range(self.num_workers):
            if i > 0:
                print(f"   ⏳ Waiting {WORKER_STAGGER_DELAY}s before starting worker {i + 1}...")
                time.sleep(WORKER_STAGGER_DELAY)
            
            p = Process(
                target=worker_process,
                args=(i, self.task_queue, self.result_queue, self.headless, 
                      INTER_REQUEST_DELAY_MIN, INTER_REQUEST_DELAY_MAX)
            )
            p.start()
            self.workers.append(p)
        
        # Wait a bit for workers to initialize
        time.sleep(3)
        print(f"\n✅ All {self.num_workers} workers started.\n")
    
    def stop_workers(self):
        """Send poison pills and wait for workers to stop."""
        print("\n🛑 Shutting down workers...")
        
        # Send poison pills
        for _ in self.workers:
            if self.task_queue:
                self.task_queue.put(None)
        
        # Wait for workers to finish
        for i, p in enumerate(self.workers):
            p.join(timeout=10)
            if p.is_alive():
                print(f"   ⚠️ Worker {i + 1} didn't stop gracefully, terminating...")
                p.terminate()
            else:
                print(f"   ✅ Worker {i + 1} stopped")
        
        self.workers.clear()
    
    def _dedupe_lead(self, lead_dict: dict) -> bool:
        """Check if lead is unique. Returns True if unique, False if duplicate."""
        # Check domain
        website = lead_dict.get('website')
        if website:
            from urllib.parse import urlparse
            try:
                domain = urlparse(website).netloc.lower()
                if domain in self._seen_domains:
                    return False
                self._seen_domains.add(domain)
            except:
                pass
        
        # Check name
        name = lead_dict.get('name', '')
        name_key = name.lower().strip()
        if name_key in self._seen_names:
            return False
        self._seen_names.add(name_key)
        
        return True
    
    def _generate_tasks(self) -> List[Tuple[str, str]]:
        """Generate all keyword × location combinations."""
        tasks = []
        for location in LOCATIONS:
            for keyword in KEYWORDS:
                tasks.append((keyword, location))
        return tasks
    
    def _process_batch(self, batch: List[dict], master_path: Path):
        """Export and upload a completed batch."""
        from utils import Lead, append_to_master, export_batch
        from webhook import send_batch_to_webhook
        from common.validator import get_website_status
        
        self.batch_number += 1
        
        # Convert dicts to Lead objects with validation
        validated_leads = []
        for ld in batch:
            status = get_website_status(ld.get('website'))
            lead = Lead(
                name=ld['name'],
                website=ld.get('website'),
                location=ld['location'],
                keyword=ld['keyword'],
                place_url=ld.get('place_url'),
                status=status
            )
            validated_leads.append(lead)
        
        # Export to CSV
        destination = OUTPUT_DIR / f"leads_batch_{self.batch_number:03d}.csv"
        export_batch(validated_leads, destination)
        append_to_master(master_path, validated_leads)
        
        # Upload to webhook
        send_batch_to_webhook(self.batch_number, validated_leads)
        
        # Summary
        print(f"\n--- Batch {self.batch_number} ({len(validated_leads)} leads) ---")
        for lead in validated_leads[:10]:  # Show first 10
            website = lead.website or "<no website>"
            print(f" • {lead.name}  |  {website}")
        if len(validated_leads) > 10:
            print(f" ... and {len(validated_leads) - 10} more")
        
        print(f"Saved batch {self.batch_number} → {destination}")
        print(f"Updated consolidated dataset → {master_path}")
    
    def run(self):
        """Main execution loop with parallel task distribution."""
        tasks = self._generate_tasks()
        total_tasks = len(tasks)
        
        if total_tasks == 0:
            print("No tasks to execute. Check LOCATIONS and KEYWORDS in config.")
            return
        
        print(f"📋 {total_tasks} search tasks to execute across {self.num_workers} workers.\n")
        
        master_path = MASTER_OUTPUT
        if master_path.exists():
            master_path.unlink()
        
        current_batch: List[dict] = []
        tasks_sent = 0
        tasks_completed = 0
        captcha_abort = False
        
        try:
            # Send initial tasks
            initial_batch = min(self.num_workers, total_tasks)
            for i in range(initial_batch):
                self.task_queue.put(tasks[i])
                tasks_sent += 1
            
            print(f"=== Started processing {initial_batch} tasks ===\n")
            
            # Process results and send more tasks
            while tasks_completed < total_tasks and not captcha_abort:
                try:
                    result = self.result_queue.get(timeout=120)  # 2 min timeout
                    tasks_completed += 1
                    
                    if result.captcha_detected:
                        print("\n🛑 CAPTCHA detected - aborting to protect session.")
                        captcha_abort = True
                        break
                    
                    if result.error and not result.captcha_detected:
                        print(f"   [W{result.worker_id + 1}] Skipping due to error")
                    else:
                        # Process leads with deduplication
                        for lead_dict in result.leads:
                            if self._dedupe_lead(lead_dict):
                                current_batch.append(lead_dict)
                    
                    # Send next task if available
                    if tasks_sent < total_tasks:
                        self.task_queue.put(tasks[tasks_sent])
                        tasks_sent += 1
                    
                    # Check if batch is ready
                    target_batch_size = BATCH_SIZE * self.num_workers  # 100 for 2 workers
                    if len(current_batch) >= target_batch_size:
                        self._process_batch(current_batch[:target_batch_size], master_path)
                        
                        # Prompt to continue
                        print(f"\nBatch {self.batch_number} ready. Press Enter for next {target_batch_size} leads or 'q' to stop:", end=" ")
                        try:
                            response = input().strip().lower()
                            if response in {"q", "quit", "exit"}:
                                print("Stopping as requested.")
                                break
                        except EOFError:
                            break
                        
                        current_batch = current_batch[target_batch_size:]
                        
                except queue.Empty:
                    print("⚠️ Timeout waiting for worker results. Checking worker status...")
                    # Check if workers are still alive
                    alive_workers = sum(1 for p in self.workers if p.is_alive())
                    if alive_workers == 0:
                        print("❌ All workers have stopped unexpectedly.")
                        break
            
            # Final batch
            if current_batch and not captcha_abort:
                self._process_batch(current_batch, master_path)
                
        finally:
            self.stop_workers()


def run_parallel(headless: bool = False):
    """Entry point for parallel scraping mode."""
    print("=" * 60)
    print("  PARALLEL MAPS SCRAPER")
    print(f"  Workers: {NUM_WORKERS} | Delay: {INTER_REQUEST_DELAY_MIN}-{INTER_REQUEST_DELAY_MAX}s | Stagger: {WORKER_STAGGER_DELAY}s")
    print("=" * 60)
    
    orchestrator = ParallelOrchestrator(headless=headless)
    
    try:
        orchestrator.start_workers()
        orchestrator.run()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user. Cleaning up...")
        orchestrator.stop_workers()
    
    print("\n✅ Parallel scraping session complete.")


if __name__ == "__main__":
    headless_arg = "--headless" in sys.argv
    run_parallel(headless=headless_arg)
