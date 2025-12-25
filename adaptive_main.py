"""Adaptive Hybrid Scraper - Main Orchestrator.

This is the entry point for the production-grade Google Maps scraper.
Uses parallel workers with job queue, buffered scrolling, and crash recovery.

Usage:
    python adaptive_main.py              # Visible browser
    python adaptive_main.py --headless   # Headless mode
    python adaptive_main.py --resume     # Resume from progress.json
"""

from __future__ import annotations

import multiprocessing as mp
import signal
import sys
import time
from pathlib import Path
from queue import Empty
from typing import Optional

from config import (
    ADAPTIVE_KEYWORDS,
    BATCH_DIR,
    NUM_WORKERS,
    POSTAL_DISTRICTS,
    PROGRESS_FILE,
    WORKER_STAGGER_DELAY,
    MAX_GRID_DEPTH,
)
from job_queue import JobQueue, Job, JobStatus
from data_pipeline import DataPipeline, get_dedup_key
from hybrid_scraper import HybridScraper, ScrapeResult, CaptchaDetected


def worker_process(
    worker_id: int,
    task_queue: mp.Queue,
    result_queue: mp.Queue,
    headless: bool,
):
    """Worker process that runs its own browser instance."""
    print(f"[Worker {worker_id}] Starting...")
    
    scraper = HybridScraper(headless=headless, worker_id=worker_id)
    try:
        scraper.start()
        print(f"[Worker {worker_id}] Browser ready")
        
        while True:
            try:
                task = task_queue.get(timeout=5)
            except Empty:
                continue
            
            if task is None:  # Poison pill
                print(f"[Worker {worker_id}] Received shutdown signal")
                break
            
            job: Job = task
            print(f"[Worker {worker_id}] Processing job: {job.keyword} @ {job.location}")
            
            try:
                result = scraper.scrape_job(
                    keyword=job.keyword,
                    location=job.location,
                    bbox=job.bbox,
                )
                result_queue.put({
                    "worker_id": worker_id,
                    "job_id": job.id,
                    "status": result.status,
                    "buffer": result.buffer,
                    "bbox": result.bbox,
                    "unique_count": result.unique_count,
                    "error": None,
                    "captcha": False,
                })
                
                # Inter-job delay for IP protection (10-15s)
                import random
                delay = random.uniform(10, 15)
                print(f"[Worker {worker_id}] Waiting {delay:.1f}s before next job...")
                time.sleep(delay)
            except CaptchaDetected:
                result_queue.put({
                    "worker_id": worker_id,
                    "job_id": job.id,
                    "status": "ERROR",
                    "buffer": [],
                    "bbox": None,
                    "unique_count": 0,
                    "error": "Captcha detected",
                    "captcha": True,
                })
            except Exception as e:
                result_queue.put({
                    "worker_id": worker_id,
                    "job_id": job.id,
                    "status": "ERROR",
                    "buffer": [],
                    "bbox": None,
                    "unique_count": 0,
                    "error": str(e),
                    "captcha": False,
                })
    finally:
        scraper.stop()
        print(f"[Worker {worker_id}] Stopped")


class AdaptiveOrchestrator:
    """Main orchestrator for adaptive hybrid scraping."""
    
    def __init__(self, headless: bool = False, num_workers: int = NUM_WORKERS):
        self.headless = headless
        self.num_workers = num_workers
        
        self.job_queue = JobQueue(PROGRESS_FILE, BATCH_DIR)
        self.pipeline = DataPipeline(BATCH_DIR, self.job_queue.global_seen)
        
        self.task_queue: Optional[mp.Queue] = None
        self.result_queue: Optional[mp.Queue] = None
        self.workers = []
        
        self._shutdown = False
    
    def _setup_signal_handlers(self):
        """Handle graceful shutdown on Ctrl+C."""
        def signal_handler(sig, frame):
            print("\n\nShutdown requested...")
            self._shutdown = True
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def start_workers(self):
        """Start worker processes."""
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        
        for i in range(self.num_workers):
            p = mp.Process(
                target=worker_process,
                args=(i, self.task_queue, self.result_queue, self.headless),
            )
            p.start()
            self.workers.append(p)
            time.sleep(WORKER_STAGGER_DELAY)  # Stagger starts
        
        print(f"Started {len(self.workers)} worker processes")
    
    def stop_workers(self):
        """Stop worker processes gracefully."""
        print("Stopping workers...")
        for _ in self.workers:
            self.task_queue.put(None)  # Poison pills
        
        for p in self.workers:
            p.join(timeout=10)
            if p.is_alive():
                p.terminate()
        
        self.workers.clear()
    
    def run(self, resume: bool = False):
        """Main execution loop."""
        self._setup_signal_handlers()
        
        # Initialize job queue
        if not resume or not PROGRESS_FILE.exists():
            print("Initializing fresh job queue...")
            self.job_queue.add_initial_jobs(ADAPTIVE_KEYWORDS, POSTAL_DISTRICTS)
        else:
            print(f"Resuming from {PROGRESS_FILE}")
        
        # Recover any pending batches from previous crash
        recovered = self.pipeline.recover_pending_batches()
        if recovered:
            print(f"Recovered {recovered} pending batches from previous run")
        
        # Start workers
        self.start_workers()
        
        jobs_in_flight = {}
        total_leads = 0
        
        try:
            while not self._shutdown:
                stats = self.job_queue.get_stats()
                print(f"\n[Stats] Pending: {stats['pending']} | Done: {stats['done']} | Split: {stats['split']} | Total Leads: {total_leads}")
                
                # Fill worker queues
                while len(jobs_in_flight) < self.num_workers:
                    job = self.job_queue.get_next_pending()
                    if not job:
                        break
                    
                    # Check max depth
                    if job.depth > MAX_GRID_DEPTH:
                        print(f"Job {job.id} exceeds max depth ({job.depth}), marking done")
                        self.job_queue.mark_done(job.id)
                        continue
                    
                    # Mark job as in-progress BEFORE dispatching to prevent duplicate dispatch
                    self.job_queue.mark_in_progress(job.id)
                    self.task_queue.put(job)
                    jobs_in_flight[job.id] = job
                
                if not jobs_in_flight and stats['pending'] == 0:
                    print("\nAll jobs complete!")
                    break
                
                # Process results
                try:
                    result = self.result_queue.get(timeout=1)
                except Empty:
                    continue
                
                job_id = result['job_id']
                job = jobs_in_flight.pop(job_id, None)
                
                if result['captcha']:
                    print(f"\n⚠️  CAPTCHA detected! Pausing...")
                    # Re-queue the job for later
                    if job:
                        self.job_queue.jobs[job_id].status = JobStatus.PENDING
                    time.sleep(60)
                    continue
                
                if result['error']:
                    print(f"\n❌ Job {job_id} error: {result['error']}")
                    continue
                
                if result['status'] == "SPLIT":
                    print(f"📊 Job {job_id} → SPLIT ({result['unique_count']} unique)")
                    
                    # Yellow Lane: Persist buffer to staging
                    buffer_file = None
                    if result['buffer']:
                        buffer_file = self.pipeline.persist_split_buffer(job_id, result['buffer'])
                    
                    # Try to create child quadrants
                    split_success = self.job_queue.mark_split(job_id, buffer_file)
                    
                    if not split_success:
                        # Cannot split (tile too small or max depth) - commit as Green Lane
                        print(f"  🟢 Cannot split further, committing {len(result['buffer'])} leads to Master")
                        new_leads = self.pipeline.commit_job_to_master(
                            job_id,
                            result['buffer'],
                        )
                        total_leads += len(new_leads)
                        self.job_queue.mark_done(job_id)
                    else:
                        print(f"  🟡 Staged {len(result['buffer'])} leads (Yellow Lane)")
                    
                elif result['status'] == "DONE":
                    print(f"✅ Job {job_id} → DONE ({result['unique_count']} unique)")
                    # Green Lane: Commit directly to Master
                    new_leads = self.pipeline.commit_job_to_master(
                        job_id,
                        result['buffer'],
                    )
                    total_leads += len(new_leads)
                    self.job_queue.mark_done(job_id)
                    
                    # Check if parent can be unioned
                    if job and job.parent_id:
                        parent_id = self.job_queue.check_parent_complete(job_id)
                        if parent_id:
                            print(f"🔗 All children of {parent_id} complete, running union...")
                            parent = self.job_queue.jobs[parent_id]
                            children_files = [
                                self.job_queue.jobs[c].buffer_file
                                for c in parent.children_ids
                                if c in self.job_queue.jobs
                            ]
                            self.pipeline.union_and_commit(
                                parent_id,
                                parent.buffer_file,
                                children_files,
                            )
        
        finally:
            self.stop_workers()
            print(f"\n{'='*50}")
            print(f"Final Stats:")
            stats = self.job_queue.get_stats()
            print(f"  Jobs: {stats['done']} done, {stats['split']} split, {stats['pending']} pending")
            print(f"  Total unique leads: {len(self.job_queue.global_seen)}")
            print(f"{'='*50}")


def main():
    """Entry point."""
    headless = "--headless" in sys.argv
    resume = "--resume" in sys.argv
    
    print("="*60)
    print("  ADAPTIVE HYBRID GOOGLE MAPS SCRAPER")
    print("="*60)
    print(f"Mode: {'Headless' if headless else 'Visible'}")
    print(f"Workers: {NUM_WORKERS}")
    print(f"Keywords: {len(ADAPTIVE_KEYWORDS)}")
    print(f"Postal Districts: {len(POSTAL_DISTRICTS)}")
    print(f"Total Jobs: {len(ADAPTIVE_KEYWORDS) * len(POSTAL_DISTRICTS)}")
    print("="*60)
    
    orchestrator = AdaptiveOrchestrator(headless=headless)
    orchestrator.run(resume=resume)


if __name__ == "__main__":
    main()
