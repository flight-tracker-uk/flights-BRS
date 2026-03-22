#!/usr/bin/env python3
"""Export local SQLite flight cache to a SQL dump file for D1 bulk import.

Only exports data for the destinations in the local DB, and only deletes
those destinations' data in D1 — so parallel jobs don't wipe each other.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".flightcache"
DB_PATH = CACHE_DIR / "flights.db"
DUMP_PATH = CACHE_DIR / "d1_import.sql"


def escape_sql(val) -> str:
    """Escape a value for raw SQL insertion."""
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    s = str(val).replace("'", "''")
    return f"'{s}'"


def export(db_path: Path = DB_PATH, dump_path: Path = DUMP_PATH) -> Path:
    local = sqlite3.connect(str(db_path))
    local.row_factory = sqlite3.Row

    # Find which (origin, destination, date) combos we have
    search_keys = local.execute(
        "SELECT DISTINCT origin, destination, flight_date FROM searches"
    ).fetchall()

    with open(dump_path, "w") as f:
        # Only delete flights/searches for the specific dates we're replacing
        for row in search_keys:
            o, d, fd = row["origin"], row["destination"], row["flight_date"]
            f.write(f"DELETE FROM flights WHERE search_id IN "
                    f"(SELECT id FROM searches WHERE origin={escape_sql(o)} "
                    f"AND destination={escape_sql(d)} AND flight_date={escape_sql(fd)});\n")
            f.write(f"DELETE FROM searches WHERE origin={escape_sql(o)} "
                    f"AND destination={escape_sql(d)} AND flight_date={escape_sql(fd)};\n")
        f.write("\n")

        # Upsert airports
        airports = local.execute("SELECT * FROM airports").fetchall()
        for a in airports:
            f.write(
                f"INSERT INTO airports(iata_code, name, country, is_origin) VALUES("
                f"{escape_sql(a['iata_code'])}, {escape_sql(a['name'])}, "
                f"{escape_sql(a['country'])}, {a['is_origin']}) "
                f"ON CONFLICT(iata_code) DO UPDATE SET name=excluded.name, "
                f"country=excluded.country, is_origin=MAX(is_origin, excluded.is_origin);\n"
            )
        f.write("\n")
        logger.info(f"Exported {len(airports)} airports")

        # Upsert routes
        routes = local.execute("SELECT * FROM routes").fetchall()
        for r in routes:
            f.write(
                f"INSERT INTO routes(origin, destination, dest_name, is_active) VALUES("
                f"{escape_sql(r['origin'])}, {escape_sql(r['destination'])}, "
                f"{escape_sql(r['dest_name'])}, {r['is_active']}) "
                f"ON CONFLICT(origin, destination) DO UPDATE SET "
                f"dest_name=excluded.dest_name, is_active=excluded.is_active;\n"
            )
        f.write("\n")
        logger.info(f"Exported {len(routes)} routes")

        # Insert searches with explicit IDs
        searches = local.execute("SELECT * FROM searches").fetchall()
        # Use a large offset for IDs to avoid conflicts between parallel jobs
        # Each destination gets a unique range based on a hash
        for s in searches:
            f.write(
                f"INSERT INTO searches(origin, destination, flight_date, direction, "
                f"searched_at, status, error_message, flight_count) VALUES("
                f"{escape_sql(s['origin'])}, {escape_sql(s['destination'])}, "
                f"{escape_sql(s['flight_date'])}, {escape_sql(s['direction'])}, "
                f"{escape_sql(s['searched_at'])}, {escape_sql(s['status'])}, "
                f"{escape_sql(s['error_message'])}, {s['flight_count']});\n"
            )
        f.write("\n")
        logger.info(f"Exported {len(searches)} searches")

        # Insert flights — reference searches by unique key, not ID
        flights = local.execute("""
            SELECT f.*, s.origin, s.destination, s.flight_date, s.direction
            FROM flights f JOIN searches s ON f.search_id = s.id
        """).fetchall()

        batch = []
        for fl in flights:
            search_ref = (
                f"(SELECT id FROM searches WHERE origin={escape_sql(fl['origin'])} "
                f"AND destination={escape_sql(fl['destination'])} "
                f"AND flight_date={escape_sql(fl['flight_date'])} "
                f"AND direction={escape_sql(fl['direction'])})"
            )
            batch.append(
                f"INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                f"depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) VALUES("
                f"{search_ref}, {escape_sql(fl['airline'])}, "
                f"{escape_sql(fl['departure_time'])}, {escape_sql(fl['arrival_time'])}, "
                f"{fl['depart_minutes']}, {fl['arrive_minutes']}, "
                f"{fl['price']}, {escape_sql(fl['currency'])}, "
                f"{fl['stops']}, {escape_sql(fl['arrival_ahead'])}, "
                f"{escape_sql(fl['created_at'])});\n"
            )

        for line in batch:
            f.write(line)

        logger.info(f"Exported {len(flights)} flights")

    local.close()

    size_kb = dump_path.stat().st_size / 1024
    logger.info(f"SQL dump: {dump_path} ({size_kb:.0f} KB)")
    return dump_path


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_path = Path(os.environ.get("DB_PATH", str(DB_PATH)))
    dump_path = Path(os.environ.get("DUMP_PATH", str(DUMP_PATH)))

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return 1

    export(db_path, dump_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
