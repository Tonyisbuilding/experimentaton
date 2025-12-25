"""Configuration for the Google Maps lead scraper."""

from pathlib import Path

# Narrow geographic targets – add more items as your addressing partner
# delivers additional micro-locations.
LOCATIONS = [
    "De Pijp, Amsterdam, Noord-Holland, Netherlands",
]

# Keywords to combine with each location. The scraper will iterate the
# cartesian product of KEYWORDS × LOCATIONS.
KEYWORDS = [
    "family office",
]

# Number of unique leads (post-deduplication) to collect before pausing to
# request operator confirmation to continue.
BATCH_SIZE = 50

# Where batch CSVs should be written.
OUTPUT_DIR = Path("output")

# Consolidated CSV containing every lead gathered during a session. Resets at
# the start of each run.
MASTER_OUTPUT = OUTPUT_DIR / "leads_full.csv"

# Webhook integration (Google Apps Script)
WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbzqD2ny083z43T6vnD2nifpYMI7RdbRXhOvrsFuhKHrJdlvF0Ealy7QwsDxJpVeSaYo/exec"
WEBHOOK_SECRET = "FMP-Scraper"
WEBHOOK_SPREADSHEET_ID = "1KrKoeun-h6eEzSK6-cc4_MUHriuabg8GyAR9b8NR7fc"
WEBHOOK_TAB_NAME = "Leads"

# Persistent Playwright profile directory so Google consent cookies survive
# across runs. Stored alongside the repo for transparency.
PLAYWRIGHT_USER_DATA = Path(".playwright-profile")

# Domains to drop because they belong to aggregators, directories, or social
# networks rather than to the business itself.
AGGREGATOR_DOMAINS = {
    "google.com",
    "maps.google.com",
    "momentumfo.com",
    "yellowpages.com",
    "yelp.com",
    "foursquare.com",
    "facebook.com",
    "linkedin.com",
    "instagram.com",
    "twitter.com",
    "tiktok.com",
    "pinterest.com",
    "detelefoongids.nl",
    "bedrijveninformatie.nl",
    "kvk.nl",
    "glassdoor.com",
    "indeed.com",
    "trulylocal.com",
    "tripadvisor.com",
    "goudenpagina.nl",
}

# How long (seconds) we idle between Map item interactions. The throttling
# logic randomises between +/- this many seconds to appear human.
MIN_DELAY_S = 0.8
MAX_DELAY_S = 1.6

# When Google presents a captcha we back off for this many seconds before
# surfacing the issue. Keeping it generous prevents lockouts.
CAPTCHA_BACKOFF_S = 45

# ============================================================================
# PARALLEL SCRAPING CONFIGURATION
# ============================================================================
# Number of parallel browser instances. Set to 1 to disable parallel mode.
NUM_WORKERS = 3

# Delay range (seconds) between requests. Actual delay is randomized within this range.
INTER_REQUEST_DELAY_MIN = 10
INTER_REQUEST_DELAY_MAX = 15

# Seconds between starting each worker to stagger initial requests.
WORKER_STAGGER_DELAY = 5

# ============================================================================
# ADAPTIVE HYBRID SCRAPER CONFIGURATION
# ============================================================================

# Trigger grid split when unique count exceeds this threshold
UNIQUE_COUNT_THRESHOLD = 60

# Number of scrolls with unchanged count before declaring plateau
PLATEAU_SCROLLS = 2

# Delayed retry: attempts and wait time when plateau detected
DELAYED_RETRY_ATTEMPTS = 4
DELAYED_RETRY_WAIT_SEC = 5

# Grid splitting parameters ("Fake Box" system)
STARTING_RADIUS_KM = 2.5        # Creates 5km wide initial box from postal code center
MIN_TILE_WIDTH_KM = 0.25        # Stop splitting at 250m (city block size)
MAX_GRID_DEPTH = 8              # Hard safety cap on recursion

# Conversion: 1 degree latitude ≈ 111 km
KM_TO_DEG = 1 / 111.0
STARTING_RADIUS_DEG = STARTING_RADIUS_KM * KM_TO_DEG
MIN_TILE_WIDTH_DEG = MIN_TILE_WIDTH_KM * KM_TO_DEG

# State persistence
PROGRESS_FILE = Path("progress.json")
BATCH_DIR = OUTPUT_DIR / "batches"
STAGING_DIR = OUTPUT_DIR / "staging"  # Yellow lane for SPLIT jobs
MASTER_CSV = OUTPUT_DIR / "master_leads.csv"  # Green lane for DONE jobs

# Keywords for adaptive scraper - Corporate businesses
ADAPTIVE_KEYWORDS = [
    "law firm",
    "accountant",
    "financial advisor",
    "insurance broker",
]

# Initial postal codes with coordinates (lat, lng)
# Format: (postal_code, city, province, lat, lng)
POSTAL_DISTRICTS = [
    ("1011", "Amsterdam", "North Holland", 52.3778, 4.9057),
    ("1406", "Bussum", "North Holland", 52.2736, 5.1586),
    ("1814", "Alkmaar", "North Holland", 52.6259, 4.7522),
    ("2341", "Oegstgeest", "South Holland", 52.1811, 4.4492),
    ("2914", "Nieuwerkerk aan den IJssel", "South Holland", 51.9725, 4.6130),
    ("3267", "Goudswaard", "South Holland", 51.7936, 4.2756),
    ("3771", "Barneveld", "Guelders", 52.1370, 5.5946),
    ("4243", "Nieuwland", "Utrecht", 51.9033, 5.0145),
    ("4702", "Roosendaal", "North Brabant", 51.5263, 4.4690),
    ("5232", "'s-Hertogenbosch", "North Brabant", 51.7057, 5.3191),
    ("5623", "Eindhoven", "North Brabant", 51.4573, 5.4735),
    ("6071", "Swalmen", "Limburg", 51.2141, 6.0425),
    ("6574", "Nijmegen", "Guelders", 51.8301, 5.9069),
    ("7046", "Vethuizen", "Guelders", 51.9149, 6.2951),
    ("7551", "Hengelo", "Overijssel", 52.2665, 6.7904),
    ("7916", "Elim", "Drenthe", 52.6808, 6.5807),
    ("8344", "Onna", "Overijssel", 52.7659, 6.1634),
    ("8651", "IJlst", "Friesland", 53.0117, 5.6226),
    ("9123", "Mitselwier", "Friesland", 53.3606, 6.0645),
    ("9488", "Zeijerveld", "Drenthe", 53.0339, 6.5221),
]


