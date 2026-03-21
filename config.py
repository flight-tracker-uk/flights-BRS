"""Configuration constants for the flight cache system."""
import os
from pathlib import Path

# Airport config — change this per repo
AIRPORT = os.environ.get("AIRPORT", "BRS")

# Paths
CACHE_DIR = Path.home() / ".flightcache"
DB_PATH = CACHE_DIR / "flights.db"
LOG_PATH = CACHE_DIR / "refresh.log"
LOCK_PATH = CACHE_DIR / "refresh.lock"

# Rate limiting — aggressive but safe
MIN_DELAY = 1.5
MAX_DELAY = 3.0
DEST_PAUSE_MIN = 3
DEST_PAUSE_MAX = 6
BATCH_COOLDOWN = 100
BATCH_PAUSE_MIN = 15
BATCH_PAUSE_MAX = 30

# Backoff on errors
BACKOFF_INITIAL = 60
BACKOFF_MULTIPLIER = 2
BACKOFF_MAX = 600
MAX_CONSECUTIVE_ERRORS = 5

# Staleness tiers (days_until_flight -> max_cache_age_hours)
STALENESS_TIERS = [
    (3, 6),
    (7, 12),
    (14, 24),
    (30, 48),
    (999, 72),
]

# Chrome TLS fingerprint versions for rotation
CHROME_VERSIONS = ["chrome_126", "chrome_127", "chrome_128", "chrome_131"]

# Google consent cookie sets for rotation
CONSENT_COOKIES = [
    "CONSENT=YES+cb.20210328-17-p0.en+FX+987; SOCS=CAISHAgDEhJnd3NfMjAyNjAzMjEtMF9SQzIaAmVuIAEaBgiVg_rNBg",
    "CONSENT=YES+cb.20210420-09-p0.en+FX+112; SOCS=CAISHAgDEhJnd3NfMjAyNjAzMjAtMF9SQzIaAmVuIAEaBgiVg_rNBg",
    "CONSENT=YES+cb.20210515-14-p0.en+FX+555; SOCS=CAISHAgDEhJnd3NfMjAyNjAzMTktMF9SQzIaAmVuIAEaBgiVg_rNBg",
]
