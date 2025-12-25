"""Webhook integration for Google Sheets via Apps Script.

Standalone version using requests library.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Any, Optional

import requests

from config import WEBHOOK_URL, WEBHOOK_SECRET

logger = logging.getLogger(__name__)


def send_batch_to_webhook(
    batch_number: int,
    leads: Iterable,
    webhook_url: str = WEBHOOK_URL,
    timeout: int = 30
) -> bool:
    """Send a batch of leads to Google Sheets via webhook.
    
    Args:
        batch_number: Batch sequence number
        leads: Iterable of Lead objects (with name, website, location, keyword, place_url, status attrs)
        webhook_url: Apps Script webhook URL
        timeout: Request timeout in seconds
        
    Returns:
        True if successful, False otherwise
    """
    leads_list = list(leads)
    ts = datetime.now(timezone.utc).isoformat()
    batch_id = f"batch-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{batch_number:03d}"
    
    # Build rows: Name, Website, Location, Keyword, Place URL, Timestamp, BatchID, Status
    rows = []
    for lead in leads_list:
        # Handle both dict and object leads
        if isinstance(lead, dict):
            name = lead.get("name", "")
            website = lead.get("website", "")
            location = lead.get("location", "")
            keyword = lead.get("keyword", "")
            place_url = lead.get("place_url", "")
            status = lead.get("status", "")
        else:
            name = getattr(lead, "name", "")
            website = getattr(lead, "website", "") or ""
            location = getattr(lead, "location", "")
            keyword = getattr(lead, "keyword", "")
            place_url = getattr(lead, "place_url", "") or ""
            status = getattr(lead, "status", "")
        
        rows.append([name, website, location, keyword, place_url, ts, batch_id, status])
    
    payload = {
        "token": WEBHOOK_SECRET,
        "batch_id": batch_id,
        "rows": rows,
    }
    
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=timeout,
            allow_redirects=True,
        )
        response.raise_for_status()
        
        text = response.text.strip().lower()
        if any(x in text for x in ["success", "added", "ok", "duplicate"]):
            print(f"[Webhook] OK (batch_id={batch_id}, {len(leads_list)} leads)")
            return True
        else:
            logger.warning(f"[Webhook] Unexpected response: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error(f"[Webhook] Timeout for batch {batch_number}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"[Webhook] Failed: {e}")
        return False
    except Exception as e:
        logger.error(f"[Webhook] Unexpected error: {e}")
        return False
