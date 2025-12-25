"""Job Queue System for Adaptive Hybrid Scraper.

Manages job state with progress.json for resumability:
- PENDING: Job waiting to be processed
- SPLIT: Job triggered density threshold, children queued
- DONE: Job completed successfully
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set
from enum import Enum

from grid_splitter import BBox, split_bbox, get_location_string


class JobStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"  # Currently being processed by a worker
    SPLIT = "SPLIT"
    DONE = "DONE"


@dataclass
class Job:
    """Represents a single scraping job."""
    id: str
    keyword: str
    location: str  # Location string for search
    bbox: Optional[BBox] = None  # Bounding box (set after split triggers)
    depth: int = 0
    status: JobStatus = JobStatus.PENDING
    parent_id: Optional[str] = None
    children_ids: List[str] = field(default_factory=list)
    buffer_file: Optional[str] = None  # Path to persisted buffer for SPLIT jobs
    center_lat: Optional[float] = None  # Center latitude (for initial grid creation)
    center_lng: Optional[float] = None  # Center longitude (for initial grid creation)
    
    def to_dict(self) -> dict:
        """Convert job to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "keyword": self.keyword,
            "location": self.location,
            "bbox": list(self.bbox) if self.bbox else None,
            "depth": self.depth,
            "status": self.status.value,
            "parent_id": self.parent_id,
            "children_ids": self.children_ids,
            "buffer_file": self.buffer_file,
            "center_lat": self.center_lat,
            "center_lng": self.center_lng,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        """Create job from dictionary."""
        return cls(
            id=data["id"],
            keyword=data["keyword"],
            location=data["location"],
            bbox=tuple(data["bbox"]) if data.get("bbox") else None,
            depth=data.get("depth", 0),
            status=JobStatus(data["status"]),
            parent_id=data.get("parent_id"),
            children_ids=data.get("children_ids", []),
            buffer_file=data.get("buffer_file"),
            center_lat=data.get("center_lat"),
            center_lng=data.get("center_lng"),
        )


def generate_job_id(keyword: str, location: str, bbox: Optional[BBox] = None) -> str:
    """Generate deterministic job ID from keyword + location/bbox."""
    if bbox:
        key = f"{keyword}|{bbox[0]:.6f},{bbox[1]:.6f},{bbox[2]:.6f},{bbox[3]:.6f}"
    else:
        key = f"{keyword}|{location}"
    return hashlib.sha256(key.encode()).hexdigest()[:12]


class JobQueue:
    """Manages the job queue with persistence for crash recovery."""
    
    def __init__(self, progress_file: Path, batch_dir: Path):
        self.progress_file = progress_file
        self.batch_dir = batch_dir
        self.jobs: Dict[str, Job] = {}
        self.global_seen: Set[str] = set()  # Dedup keys of committed leads
        self._load_progress()
    
    def _load_progress(self):
        """Load progress from file if exists."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, "r") as f:
                    data = json.load(f)
                    for job_id, job_data in data.get("jobs", {}).items():
                        self.jobs[job_id] = Job.from_dict(job_data)
                    self.global_seen = set(data.get("global_seen", []))
                print(f"Loaded {len(self.jobs)} jobs from progress file")
            except Exception as e:
                print(f"Warning: Could not load progress file: {e}")
    
    def _save_progress(self):
        """Save current progress to file."""
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "jobs": {job_id: job.to_dict() for job_id, job in self.jobs.items()},
            "global_seen": list(self.global_seen),
        }
        # Atomic write: write to temp file then rename
        temp_file = self.progress_file.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2)
        temp_file.rename(self.progress_file)
    
    def add_initial_jobs(self, keywords: List[str], postal_districts: List[tuple]):
        """Initialize queue with keyword × location combinations.
        
        Initial jobs use TEXT SEARCH (bbox=None). Coordinates are stored
        for later use when a split is triggered.
        
        Args:
            keywords: List of search keywords
            postal_districts: List of tuples:
                - (postal_code, city, province, lat, lng) - with coordinates
        """
        for district in postal_districts:
            if len(district) >= 5:
                postal_code, city, province, lat, lng = district[:5]
            else:
                postal_code, city, province = district[:3]
                lat, lng = None, None
            
            location = get_location_string(postal_code, city, province)
            
            for keyword in keywords:
                job_id = generate_job_id(keyword, location)
                if job_id not in self.jobs:
                    self.jobs[job_id] = Job(
                        id=job_id,
                        keyword=keyword,
                        location=location,
                        bbox=None,  # TEXT SEARCH FIRST - no coordinates
                        depth=0,
                        status=JobStatus.PENDING,
                    )
                    # Store lat/lng for later grid creation on split
                    if lat and lng:
                        self.jobs[job_id].center_lat = lat
                        self.jobs[job_id].center_lng = lng
        self._save_progress()
        print(f"Queue initialized with {len(self.jobs)} jobs (text search mode)")
    
    def get_next_pending(self) -> Optional[Job]:
        """Get next pending job, prioritizing children (depth-first)."""
        pending = [j for j in self.jobs.values() if j.status == JobStatus.PENDING]
        if not pending:
            return None
        # Sort by depth descending (children first)
        pending.sort(key=lambda j: j.depth, reverse=True)
        return pending[0]
    
    def mark_in_progress(self, job_id: str):
        """Mark job as in progress (being processed by a worker)."""
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.IN_PROGRESS
            # Don't save to file - IN_PROGRESS is transient state
    
    def mark_done(self, job_id: str):
        """Mark job as completed."""
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.DONE
            self._save_progress()
    
    def create_initial_bbox(self, job: Job) -> Optional[BBox]:
        """Create initial 'Fake Box' from job's center coordinates.
        
        Uses STARTING_RADIUS_DEG from config to create a 5km wide box.
        """
        from config import STARTING_RADIUS_DEG
        
        if job.center_lat is None or job.center_lng is None:
            return None
        
        # Create bbox centered on the stored coordinates
        return (
            job.center_lat + STARTING_RADIUS_DEG,  # north
            job.center_lng + STARTING_RADIUS_DEG,  # east
            job.center_lat - STARTING_RADIUS_DEG,  # south
            job.center_lng - STARTING_RADIUS_DEG,  # west
        )
    
    def get_tile_width_deg(self, bbox: BBox) -> float:
        """Calculate tile width in degrees (for checking MIN_TILE_WIDTH)."""
        north, east, south, west = bbox
        return east - west  # Longitude span
    
    def can_split_further(self, bbox: BBox, depth: int) -> bool:
        """Check if a bbox can be split further based on physics and depth limits."""
        from config import MIN_TILE_WIDTH_DEG, MAX_GRID_DEPTH
        
        # Check depth limit
        if depth >= MAX_GRID_DEPTH:
            return False
        
        # Check tile size limit (250m minimum)
        tile_width = self.get_tile_width_deg(bbox)
        if tile_width <= MIN_TILE_WIDTH_DEG:
            return False
        
        return True
    
    def mark_split(self, job_id: str, buffer_file: Optional[str] = None) -> bool:
        """Mark job as split and create child jobs.
        
        For depth=0 jobs, creates initial 'Fake Box' from center coordinates.
        Returns False if cannot split (tile too small or max depth).
        
        Args:
            job_id: The job that exceeded density threshold
            buffer_file: Path to persisted buffer (Yellow Lane staging)
        
        Returns:
            True if split successful, False if cannot split further
        """
        job = self.jobs.get(job_id)
        if not job:
            return False
        
        # For depth=0 jobs, create initial bbox from center coordinates
        if job.bbox is None:
            bbox = self.create_initial_bbox(job)
            if bbox is None:
                print(f"  No coordinates for {job_id}, cannot split")
                return False
        else:
            bbox = job.bbox
        
        # Check if we can split further
        if not self.can_split_further(bbox, job.depth):
            tile_km = self.get_tile_width_deg(bbox) * 111
            print(f"  Cannot split further: tile={tile_km:.2f}km, depth={job.depth}")
            return False
        
        job.status = JobStatus.SPLIT
        job.bbox = bbox
        job.buffer_file = buffer_file
        
        # Create 4 non-overlapping child quadrants (NW, NE, SW, SE)
        child_bboxes = split_bbox(bbox)
        for child_bbox in child_bboxes:
            child_id = generate_job_id(job.keyword, "", child_bbox)
            if child_id not in self.jobs:
                child_job = Job(
                    id=child_id,
                    keyword=job.keyword,
                    location=job.location,  # Keep original location for context
                    bbox=child_bbox,
                    depth=job.depth + 1,
                    status=JobStatus.PENDING,
                    parent_id=job_id,
                )
                self.jobs[child_id] = child_job
                job.children_ids.append(child_id)
        
        self._save_progress()
        tile_km = self.get_tile_width_deg(bbox) * 111
        print(f"Job {job_id} split into {len(job.children_ids)} children at depth {job.depth + 1} (tile={tile_km:.2f}km)")
        return True

    
    def add_seen_keys(self, keys: List[str]):
        """Add dedup keys to global history."""
        self.global_seen.update(keys)
        self._save_progress()
    
    def is_seen(self, key: str) -> bool:
        """Check if dedup key was already seen."""
        return key in self.global_seen
    
    def get_pending_count(self) -> int:
        """Count remaining pending jobs."""
        return sum(1 for j in self.jobs.values() if j.status == JobStatus.PENDING)
    
    def get_done_count(self) -> int:
        """Count completed jobs."""
        return sum(1 for j in self.jobs.values() if j.status == JobStatus.DONE)
    
    def get_split_count(self) -> int:
        """Count split jobs."""
        return sum(1 for j in self.jobs.values() if j.status == JobStatus.SPLIT)
    
    def get_stats(self) -> dict:
        """Get queue statistics."""
        return {
            "pending": self.get_pending_count(),
            "done": self.get_done_count(),
            "split": self.get_split_count(),
            "total_jobs": len(self.jobs),
            "total_seen": len(self.global_seen),
        }
    
    def check_parent_complete(self, job_id: str) -> Optional[str]:
        """Check if all siblings of a job are done, return parent_id if so.
        
        Used to trigger union of parent + children buffers.
        """
        job = self.jobs.get(job_id)
        if not job or not job.parent_id:
            return None
        
        parent = self.jobs.get(job.parent_id)
        if not parent:
            return None
        
        # Check if all children are done
        all_done = all(
            self.jobs.get(child_id, Job(id="", keyword="", location="")).status == JobStatus.DONE
            for child_id in parent.children_ids
        )
        
        return parent.id if all_done else None
