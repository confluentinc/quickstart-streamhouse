"""River Air flight recovery app: operations and passenger views over Lightning Tables.

Reads current state through Lightning Queries (the Global API key stays server-side;
the browser only talks to this backend). Passenger selections and bookings are written
back to the passenger_recommendations topic through Kafka, and show up on the next read.

Run:
    uv run airport-app                 # http://127.0.0.1:8000
    uv run airport-app --port 9000
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from scripts import setup_rtce as rtce
from scripts.airport_datagen import HUB, OFFERS, SEATS, Publisher
from scripts.terraform import extract_kafka_credentials, get_project_root

STATIC = Path(__file__).parent / "static"
PAGE_SIZE = rtce.LIGHTNING_MAX_ROWS
WINDOW_BEFORE = timedelta(hours=18)
WINDOW_AFTER = timedelta(hours=6)
DELAYED_MINUTES = 15
STAGES = {"BOARDING": 1, "DEPARTED": 2, "LANDED": 2}  # how far along a flight's day is; earlier statuses are 0


class Deployment:
    """Lazily-loaded Lightning connection details for the live deployment.

    Everything comes from terraform state + credentials.env, like setup-rtce, so the
    app targets whatever was deployed with no extra configuration.
    """

    def __init__(self) -> None:
        self._infra: dict[str, str] | None = None
        self._rtce_key: str | None = None
        self._rtce_secret: str | None = None

    def _load_rtce(self) -> None:
        if self._infra is not None:
            return
        creds_file = rtce._find_credentials_file()
        creds = rtce._load_env_file(creds_file)
        infra = rtce._get_infra(creds_file)
        self._rtce_key = creds.get(rtce._CRED_KEY, "")
        self._rtce_secret = creds.get(rtce._CRED_SECRET, "")
        if not self._rtce_key or not self._rtce_secret:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"No RTCE/Lightning Global API key in credentials.env "
                    f"({rtce._CRED_KEY}). Run `uv run setup-rtce` first."
                ),
            )
        self._infra = infra

    @property
    def lightning_url(self) -> str:
        self._load_rtce()
        region = self._infra["region"]
        cloud = self._infra.get("cloud", "aws").lower()
        return f"https://sql.{region}.{cloud}.confluent.cloud/query/v1alpha1"

    def lightning_query(
        self, topic: str, key: str | None = None, limit: int = PAGE_SIZE,
        where=(), order_by: tuple[str, str] | None = None,
    ) -> list[dict]:
        """Run one Lightning Query; rows come back with lowercase column names."""
        query = rtce._build_lightning_query(topic, key=key, limit=limit, where=where, order_by=order_by)
        self._load_rtce()
        try:
            resp = requests.post(
                self.lightning_url,
                auth=(self._rtce_key, self._rtce_secret),
                json={
                    "catalog_name": self._infra["env_id"],
                    "database_name": self._infra["cluster_id"],
                    "query": query,
                },
                timeout=15,
            )
        except requests.RequestException as exc:
            raise HTTPException(status_code=502, detail=f"Lightning request failed: {exc}") from exc
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Lightning query returned HTTP {resp.status_code}: {resp.text[:300]}",
            )
        body = resp.json().get("result", {})
        columns = [c["name"].lower() for c in body.get("schema", {}).get("columns", [])]
        return [dict(zip(columns, row, strict=False)) for row in body.get("data", [])]

    def lightning_scan(self, topic: str, where=()) -> list[dict]:
        """Return every matching row by paging on KEY, since each query caps at 200 rows."""
        rows: list[dict] = []
        last: str | None = None
        while True:
            conditions = [*where, ("key", ">", last)] if last is not None else list(where)
            page = self.lightning_query(topic, limit=PAGE_SIZE, where=conditions, order_by=("key", "ASC"))
            rows.extend(page)
            if len(page) < PAGE_SIZE:
                return rows
            next_last = page[-1].get("key")
            if next_last is None or next_last == last:
                return rows
            last = next_last


deployment = Deployment()
_pool = ThreadPoolExecutor(max_workers=8)  # runs a request's independent Lightning reads side by side
_publisher: Publisher | None = None
app = FastAPI(title="River Air flight recovery", docs_url=None, redoc_url=None)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _rows(topic: str, key: str | None = None, limit: int = PAGE_SIZE,
          where=(), order_by: tuple[str, str] | None = None) -> list[dict]:
    return deployment.lightning_query(topic, key=key, limit=limit, where=where, order_by=order_by)


def _int(value) -> int:
    if value in (None, ""):
        return 0
    return int(Decimal(str(value)))


def _timestamp(value) -> datetime | None:
    """Parse None, a datetime, epoch millis, or a Lightning string to naive UTC."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)) or str(value).isdigit():
        return datetime.fromtimestamp(int(value) / 1000, timezone.utc).replace(tzinfo=None)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.replace(tzinfo=None)


def _get_publisher() -> Publisher:
    global _publisher
    if _publisher is None:
        _publisher = Publisher(extract_kafka_credentials("aws", get_project_root()), dry_run=False)
    return _publisher


def _write_offer(offer: dict) -> None:
    hotel_cost = offer.get("hotel_cost")
    publisher = _get_publisher()
    publisher.publish(OFFERS, offer["key"], {
        "passenger_id": offer["passenger_id"],
        "recommended_flight_id": offer["recommended_flight_id"],
        "hotel_name": offer.get("hotel_name") or None,
        "status": offer["status"],
        "hotel_cost": Decimal(str(hotel_cost)) if hotel_cost not in (None, "") else None,
        "impacted_at": _timestamp(offer.get("impacted_at")),
        "recommended_at": _timestamp(offer.get("recommended_at")),
    })
    publisher.flush()


def _passenger_offers(passenger_id: str) -> list[dict]:
    return sorted(_rows(OFFERS, limit=3, where=[("passenger_id", "=", passenger_id)]),
                  key=lambda row: row["key"])


def _hotels() -> dict[str, dict]:
    return {row["key"]: row for row in _rows("hotel_inventory")}


def _hotel_options() -> list[dict]:
    """Every hotel's rooms left and rate, cheapest first (the order select_offer replaces a sold-out hotel in)."""
    return sorted(({**row, "available_rooms": _int(row.get("available_rooms"))} for row in _hotels().values()),
                  key=lambda row: Decimal(str(row["nightly_rate"])))


def _scheduled_rows(topic: str, start: datetime, end: datetime) -> list[dict]:
    """Every row scheduled in [start, end), paging on scheduled_time past Lightning's 200-row cap."""
    rows: dict[tuple, dict] = {}
    since = start
    while True:
        page = _rows(topic, where=[("scheduled_time", ">=", since), ("scheduled_time", "<", end)],
                     order_by=("scheduled_time", "ASC"))
        rows.update((tuple(row.items()), row) for row in page)  # each page repeats the last one's final time
        last = _timestamp(page[-1].get("scheduled_time")) if page else None
        if len(page) < PAGE_SIZE or last is None or last == since:
            return list(rows.values())
        since = last


def _live_copies(flights: list[dict], impact: dict[str, dict]) -> list[dict]:
    """One row per flight.

    After Lightning is enabled on flight_status, it can keep a frozen copy of a flight's row beside the live
    one, and no column tells them apart. The copy whose delay flight_impact reports wins, since Flink keeps
    flight_impact from the live topic. Until Flink catches up with a change, the copy furthest along wins.
    """
    copies: dict[str, list[dict]] = {}
    for row in flights:
        copies.setdefault(row["key"], []).append(row)
    chosen = []
    for key, rows in copies.items():
        reported = impact.get(key, {}).get("delay_minutes")
        matching = [row for row in rows if reported not in (None, "") and row["delay_minutes"] == _int(reported)]
        chosen.append(max(matching or rows, key=lambda row: (
            STAGES.get(row.get("status"), 0), row["estimated_time"] or datetime.min)))
    return chosen


def _flights(key: str | None = None, window: tuple[datetime, datetime] | None = None) -> list[dict]:
    """One flight, or every flight scheduled in a (start, end) window, one row each in scheduled order.

    Status and delay come from flight_status (live within seconds), the at-risk count from flight_impact
    (about a minute behind).
    """
    def read(topic: str) -> list[dict]:
        # A key lookup asks for more than one row because flight_status can hold a stale copy (_live_copies).
        return _rows(topic, key=key, limit=10) if key is not None else _scheduled_rows(topic, *window)

    impact = _pool.submit(read, "flight_impact")
    flights = []
    for row in read("flight_status"):
        scheduled, estimated = _timestamp(row.get("scheduled_time")), _timestamp(row.get("estimated_time"))
        delay = round((estimated - scheduled) / timedelta(minutes=1)) if scheduled and estimated else 0
        flights.append({**row, "estimated_time": estimated, "delay_minutes": delay})
    impact = {row["key"]: row for row in impact.result()}
    flights = sorted(_live_copies(flights, impact),
                     key=lambda row: (_timestamp(row.get("scheduled_time")) or datetime.min, row["key"]))
    return [{**row, "affected_passengers": _int(impact.get(row["key"], {}).get("affected_passengers"))}
            for row in flights]


@app.get("/api/state")
def state():
    now = _now()
    booked = _pool.submit(deployment.lightning_scan, OFFERS,
                          [("status", "=", "BOOKED"), ("recommended_at", ">=", now - WINDOW_BEFORE)])
    flights = _flights(window=(now - WINDOW_BEFORE, now + WINDOW_AFTER))
    booked = booked.result()
    delays = sorted(flights, key=lambda row: (-row["delay_minutes"], row.get("key") or ""))[:10]
    return {
        "now": now,
        "flights": flights,
        "delays": delays,
        "counts": {
            "affected": sum(row["affected_passengers"] for row in flights),
            "booked": len(booked),
            "flights": len(flights),
            "delayed": sum(1 for row in flights if row["delay_minutes"] >= DELAYED_MINUTES),
        },
    }


@app.get("/api/flight/{flight_id}")
def flight(flight_id: str):
    rows = _flights(key=flight_id)
    if not rows:
        raise HTTPException(status_code=404, detail="Flight not found")
    row = rows[0]
    row["arrival"] = row.get("destination") == HUB
    # An arrival carries its passengers' inbound leg; a departure carries the ones connecting onto it.
    leg = "inbound" if row["arrival"] else "connecting"
    passengers = deployment.lightning_scan("passenger_state", where=[(f"{leg}_flight_id", "=", flight_id)])
    for passenger in passengers:
        passenger["seat"] = passenger.get(f"{leg}_seat")
    passengers.sort(key=lambda row: (row.get("risk") != "HIGH", row.get("key") or ""))
    return {"flight": row, "passengers": passengers, "seats": SEATS}


@app.get("/api/passenger/{passenger_id}")
def passenger(passenger_id: str):
    rows = _rows("passenger_state", key=passenger_id, limit=1)
    if not rows:
        raise HTTPException(status_code=404, detail="Passenger not found")
    return {"passenger": rows[0], "offers": _passenger_offers(passenger_id), "hotels": _hotel_options()}


@app.post("/api/passenger/{passenger_id}/select/{offer_id}")
def select_offer(passenger_id: str, offer_id: str):
    offers = _passenger_offers(passenger_id)
    chosen = next((row for row in offers if row["key"] == offer_id), None)
    if chosen is None or chosen.get("status") not in {"OFFERED", "SELECTED"}:
        raise HTTPException(status_code=409, detail="Offer is unavailable")
    if chosen.get("hotel_name"):
        hotels = _hotels()
        hotel = hotels.get(chosen["hotel_name"])
        if not hotel or _int(hotel.get("available_rooms")) < 1:
            available = sorted((row for row in hotels.values() if _int(row.get("available_rooms")) > 0),
                               key=lambda row: Decimal(str(row["nightly_rate"])))
            # The passenger's other offer keeps its hotel, so the replacement is a third hotel when one has rooms.
            other = {row.get("hotel_name") for row in offers
                     if row is not chosen and row.get("status") in {"OFFERED", "SELECTED"}}
            options = [row for row in available if row["key"] not in other] or available
            if not options:
                raise HTTPException(status_code=409, detail="No hotel rooms available")
            sold_out = chosen["hotel_name"]
            chosen["hotel_name"] = options[0]["key"]
            chosen["hotel_cost"] = options[0]["nightly_rate"]
            chosen["status"] = "OFFERED"  # the passenger picks again from the new options
            _write_offer(chosen)
            return {**chosen, "sold_out_hotel": sold_out}
    chosen["status"] = "SELECTED"
    _write_offer(chosen)
    return chosen


@app.post("/api/passenger/{passenger_id}/book/{offer_id}")
def book_offer(passenger_id: str, offer_id: str):
    offers = _passenger_offers(passenger_id)
    chosen = next((row for row in offers if row["key"] == offer_id), None)
    if chosen is None or chosen.get("status") != "SELECTED":
        raise HTTPException(status_code=409, detail="Select this offer first")
    if chosen.get("hotel_name"):
        hotel = _hotels().get(chosen["hotel_name"], {})
        if _int(hotel.get("available_rooms")) < 1:
            raise HTTPException(status_code=409, detail="Hotel sold out; select again for an alternative")
    chosen["status"] = "BOOKED"
    _write_offer(chosen)
    for offer in offers:
        if offer["key"] != offer_id and offer.get("status") in {"OFFERED", "SELECTED"}:
            offer["status"] = "CLOSED"
            _write_offer(offer)
    return chosen


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="uv run airport-app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
