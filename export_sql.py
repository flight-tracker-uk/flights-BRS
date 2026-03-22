#!/usr/bin/env python3
"""Export local SQLite flight cache to a SQL dump file for D1 bulk import."""
from __future__ import annotations

import logging
import os
import re
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
    """Export the SQLite database to a SQL dump file for D1 import."""
    local = sqlite3.connect(str(db_path))
    local.row_factory = sqlite3.Row

    with open(dump_path, "w") as f:
        # Clear existing data
        f.write("DELETE FROM flights;\n")
        f.write("DELETE FROM searches;\n")
        f.write("DELETE FROM routes;\n")
        f.write("DELETE FROM airports;\n\n")

        # Export airports
        airports = local.execute("SELECT * FROM airports").fetchall()
        for a in airports:
            f.write(
                f"INSERT INTO airports(iata_code, name, country, is_origin) VALUES("
                f"{escape_sql(a['iata_code'])}, {escape_sql(a['name'])}, "
                f"{escape_sql(a['country'])}, {a['is_origin']});\n"
            )
        f.write("\n")
        logger.info(f"Exported {len(airports)} airports")

        # Export routes
        routes = local.execute("SELECT * FROM routes").fetchall()
        for r in routes:
            f.write(
                f"INSERT INTO routes(origin, destination, dest_name, is_active) VALUES("
                f"{escape_sql(r['origin'])}, {escape_sql(r['destination'])}, "
                f"{escape_sql(r['dest_name'])}, {r['is_active']});\n"
            )
        f.write("\n")
        logger.info(f"Exported {len(routes)} routes")

        # Export searches
        searches = local.execute("SELECT * FROM searches").fetchall()
        for s in searches:
            f.write(
                f"INSERT INTO searches(id, origin, destination, flight_date, direction, "
                f"searched_at, status, error_message, flight_count) VALUES("
                f"{s['id']}, {escape_sql(s['origin'])}, {escape_sql(s['destination'])}, "
                f"{escape_sql(s['flight_date'])}, {escape_sql(s['direction'])}, "
                f"{escape_sql(s['searched_at'])}, {escape_sql(s['status'])}, "
                f"{escape_sql(s['error_message'])}, {s['flight_count']});\n"
            )
        f.write("\n")
        logger.info(f"Exported {len(searches)} searches")

        # Export flights in batches for efficiency
        flights = local.execute("SELECT * FROM flights").fetchall()
        batch = []
        for fl in flights:
            batch.append(
                f"({fl['search_id']}, {escape_sql(fl['airline'])}, "
                f"{escape_sql(fl['departure_time'])}, {escape_sql(fl['arrival_time'])}, "
                f"{fl['depart_minutes']}, {fl['arrive_minutes']}, "
                f"{fl['price']}, {escape_sql(fl['currency'])}, "
                f"{fl['stops']}, {escape_sql(fl['arrival_ahead'])}, "
                f"{escape_sql(fl['created_at'])})"
            )
            if len(batch) >= 50:
                vals = ",\n  ".join(batch)
                f.write(
                    f"INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                    f"depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) VALUES\n  {vals};\n"
                )
                batch = []

        if batch:
            vals = ",\n  ".join(batch)
            f.write(
                f"INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                f"depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) VALUES\n  {vals};\n"
            )

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
