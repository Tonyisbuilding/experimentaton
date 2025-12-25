"""Grid splitting utilities for adaptive geographic coverage.

Splits bounding boxes into quadrants when density threshold is exceeded.
"""

from __future__ import annotations

from typing import List, Tuple

# Type alias for bounding boxes: (north, east, south, west)
BBox = Tuple[float, float, float, float]


def split_bbox(bbox: BBox) -> List[BBox]:
    """Split a bounding box into 4 quadrants: NE, NW, SE, SW.
    
    Args:
        bbox: Tuple of (north, east, south, west) coordinates
        
    Returns:
        List of 4 bounding boxes covering the same area
    """
    n, e, s, w = bbox
    mid_lat = (n + s) / 2
    mid_lng = (e + w) / 2
    
    return [
        (n, e, mid_lat, mid_lng),      # NE quadrant
        (n, mid_lng, mid_lat, w),      # NW quadrant
        (mid_lat, e, s, mid_lng),      # SE quadrant
        (mid_lat, mid_lng, s, w),      # SW quadrant
    ]


def bbox_to_search_url(keyword: str, bbox: BBox) -> str:
    """Convert keyword + bbox into a Google Maps search URL.
    
    Google Maps uses the format:
    https://www.google.com/maps/search/{query}/@{lat},{lng},{zoom}z
    
    For bbox searches, we center on the bbox midpoint.
    """
    from urllib.parse import quote_plus
    
    n, e, s, w = bbox
    center_lat = (n + s) / 2
    center_lng = (e + w) / 2
    
    # Estimate zoom level from bbox size (rough heuristic)
    lat_span = abs(n - s)
    zoom = max(10, min(18, int(16 - lat_span * 50)))
    
    encoded_keyword = quote_plus(keyword)
    return f"https://www.google.com/maps/search/{encoded_keyword}/@{center_lat},{center_lng},{zoom}z"


def postal_code_to_bbox(postal_code: str, city: str, province: str, country: str = "Netherlands") -> BBox:
    """Convert a postal code to an approximate bounding box.
    
    Uses a fixed-size box around the postal code centroid.
    For more accurate results, this could integrate with a geocoding API.
    
    Args:
        postal_code: The postal code (e.g., "1011")
        city: City name
        province: Province name
        country: Country name (default: Netherlands)
        
    Returns:
        Approximate bounding box for the postal district
    """
    # Default bbox size: ~2km x 2km (suitable for postal districts)
    # This gives roughly 0.018 degrees latitude and longitude
    HALF_SIZE_LAT = 0.009  # ~1km in latitude
    HALF_SIZE_LNG = 0.014  # ~1km in longitude (varies by latitude)
    
    # For now, we'll use the location string approach instead of hardcoded coordinates
    # The hybrid scraper will use the postal code as a search location
    # and extract the map viewport from the results
    
    # Placeholder: return a dummy bbox that will be refined when we navigate
    # This is a temporary solution until we integrate proper geocoding
    return (52.38, 4.92, 52.36, 4.88)  # Amsterdam default


def get_location_string(postal_code: str, city: str, province: str, country: str = "Netherlands") -> str:
    """Format a location string for Google Maps search.
    
    Args:
        postal_code: The postal code
        city: City name
        province: Province name
        country: Country name
        
    Returns:
        Formatted location string like "1011, Amsterdam, North Holland, Netherlands"
    """
    return f"{postal_code}, {city}, {province}, {country}"
