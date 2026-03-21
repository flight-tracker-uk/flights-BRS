#!/usr/bin/env python3
"""Sync local SQLite flight cache to Cloudflare D1."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

CF_API_BASE = "https://api.cloudflare.com/client/v4"


class D1Sync:
    def __init__(self):
        self.api_token = os.environ["CLOUDFLARE_API_TOKEN"]
        self.account_id = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        self.database_id = os.environ["CLOUDFLARE_D1_DATABASE_ID"]
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def _query(self, sql: str, params: list = None) -> dict:
        url = f"{CF_API_BASE}/accounts/{self.account_id}/d1/database/{self.database_id}/query"
        body = {"sql": sql}
        if params:
            body["params"] = params
        resp = requests.post(url, headers=self.headers, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _batch_query(self, statements: list) -> dict:
        """Execute multiple SQL statements in a batch."""
        url = f"{CF_API_BASE}/accounts/{self.account_id}/d1/database/{self.database_id}/query"
        # D1 REST API accepts array of statement objects
        body = [{"sql": s["sql"], "params": s.get("params", [])} for s in statements]
        resp = requests.post(url, headers=self.headers, json=body, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def sync(self, db_path: str):
        """Sync all data from local SQLite to Cloudflare D1."""
        local = sqlite3.connect(db_path)
        local.row_factory = sqlite3.Row

        # Sync airports
        airports = local.execute("SELECT * FROM airports").fetchall()
        logger.info(f"Syncing {len(airports)} airports...")
        for a in airports:
            self._query(
                "INSERT INTO airports(iata_code, name, country, is_origin) VALUES(?,?,?,?) "
                "ON CONFLICT(iata_code) DO UPDATE SET name=excluded.name, country=excluded.country, "
                "is_origin=MAX(is_origin, excluded.is_origin)",
                [a["iata_code"], a["name"], a["country"], a["is_origin"]],
            )

        # Sync routes
        routes = local.execute("SELECT * FROM routes").fetchall()
        logger.info(f"Syncing {len(routes)} routes...")
        for r in routes:
            self._query(
                "INSERT INTO routes(origin, destination, dest_name, is_active) VALUES(?,?,?,?) "
                "ON CONFLICT(origin, destination) DO UPDATE SET dest_name=excluded.dest_name, is_active=excluded.is_active",
                [r["origin"], r["destination"], r["dest_name"], r["is_active"]],
            )

        # Sync searches and flights
        searches = local.execute("SELECT * FROM searches").fetchall()
        logger.info(f"Syncing {len(searches)} searches...")

        for s in searches:
            # Upsert search
            self._query(
                "INSERT INTO searches(origin, destination, flight_date, direction, searched_at, status, error_message, flight_count) "
                "VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(origin, destination, flight_date, direction) DO UPDATE SET "
                "searched_at=excluded.searched_at, status=excluded.status, error_message=excluded.error_message, "
                "flight_count=excluded.flight_count",
                [s["origin"], s["destination"], s["flight_date"], s["direction"],
                 s["searched_at"], s["status"], s["error_message"], s["flight_count"]],
            )

            # Get the D1 search ID
            result = self._query(
                "SELECT id FROM searches WHERE origin=? AND destination=? AND flight_date=? AND direction=?",
                [s["origin"], s["destination"], s["flight_date"], s["direction"]],
            )
            d1_search_id = result["result"][0]["results"][0]["id"]

            # Delete old flights for this search in D1
            self._query("DELETE FROM flights WHERE search_id=?", [str(d1_search_id)])

            # Get local flights for this search
            flights = local.execute("SELECT * FROM flights WHERE search_id=?", (s["id"],)).fetchall()

            # Insert flights into D1
            for f in flights:
                self._query(
                    "INSERT INTO flights(search_id, airline, departure_time, arrival_time, "
                    "depart_minutes, arrive_minutes, price, currency, stops, arrival_ahead, created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    [str(d1_search_id), f["airline"], f["departure_time"], f["arrival_time"],
                     str(f["depart_minutes"]), str(f["arrive_minutes"]), str(f["price"]),
                     f["currency"], str(f["stops"]), f["arrival_ahead"], f["created_at"]],
                )

        local.close()
        logger.info("Sync to Cloudflare D1 complete!")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    db_path = os.path.expanduser("~/.flightcache/flights.db")
    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        return 1

    syncer = D1Sync()
    syncer.sync(db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
