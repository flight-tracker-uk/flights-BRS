#!/usr/bin/env python3
"""
Flight Cache Refresher — populates flight data from Google Flights.

Designed to run as a GitHub Actions cron job or locally.

Usage:
    python refresher.py --month 2026-04
    python refresher.py --month 2026-04 --destinations AGP,CDG
"""
from __future__ import annotations

import argparse
import atexit
import logging
import os
import signal
import sys
import time

from config import CACHE_DIR, LOCK_PATH, LOG_PATH, AIRPORT
from cache_db import FlightCache
from destinations import get_destinations, get_airport_name
from refresh_worker import run_refresh

logger = logging.getLogger(__name__)


def _acquire_lock() -> bool:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        try:
            pid = int(LOCK_PATH.read_text().strip())
            os.kill(pid, 0)
            logger.error(f"Another refresh is running (PID {pid})")
            return False
        except (ProcessLookupError, ValueError):
            logger.warning("Stale lock file found, taking over")
    LOCK_PATH.write_text(str(os.getpid()))
    atexit.register(lambda: LOCK_PATH.unlink(missing_ok=True))
    return True


def _setup_logging(verbose: bool):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    handlers = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(str(LOG_PATH)))
    except Exception:
        pass
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )
    logging.getLogger("primp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> int:
    parser = argparse.ArgumentParser(description="Flight cache refresher")
    parser.add_argument("--month", required=True, help="Month to refresh (YYYY-MM)")
    parser.add_argument("--airport", default=AIRPORT, help=f"Origin airport (default: {AIRPORT})")
    parser.add_argument("--destinations", help="Comma-separated IATA codes (default: all)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    _setup_logging(args.verbose)

    if not _acquire_lock():
        return 1

    airport = args.airport.upper()
    if args.destinations:
        codes = [c.strip().upper() for c in args.destinations.split(",")]
        all_dests = get_destinations(airport)
        destinations = {c: all_dests.get(c, c) for c in codes}
    else:
        destinations = get_destinations(airport)

    if not destinations:
        logger.error(f"No destinations configured for {airport}")
        return 1

    cache = FlightCache()
    cache.upsert_airport(airport, get_airport_name(airport), is_origin=True)
    for code, name in destinations.items():
        cache.upsert_airport(code, name)

    is_ci = os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS")
    last_log_time = [0]

    def on_progress(current, total, o, d, flight_date, direction, completed, failed):
        import time as _t
        pct = current / total * 100
        msg = f"[{pct:5.1f}%] {current}/{total} | {o}->{d} {flight_date} {direction} | done={completed} fail={failed}"
        if is_ci:
            # In CI: print every 10th search or every 30 seconds so logs are visible
            now = _t.time()
            if current % 10 == 0 or current == total or (now - last_log_time[0]) > 30:
                print(msg, flush=True)
                last_log_time[0] = now
        else:
            print(f"\r  {msg}", end="", flush=True)

    logger.info(f"Starting refresh: {airport} -> {len(destinations)} destinations, {args.month}")
    start = time.time()

    stats = run_refresh(
        cache=cache, origin=airport, destinations=destinations,
        month=args.month, progress_callback=on_progress,
    )

    elapsed = time.time() - start
    print()
    db_stats = cache.get_stats()
    cache.close()

    logger.info(
        f"Refresh finished in {elapsed/60:.1f} minutes. "
        f"Completed: {stats['completed']}, Failed: {stats['failed']}. "
        f"DB: {db_stats['searches']} searches, {db_stats['flights']} flights."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
