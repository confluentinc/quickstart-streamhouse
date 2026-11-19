import base64
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from scripts import airport_app
from scripts.airport_app import Deployment
from scripts.setup_rtce import _build_lightning_query

NOW = datetime(2026, 9, 28, 12, 0, 0)


# --- query builder -----------------------------------------------------------


def test_builder_escapes_quotes_and_renders_key():
    assert _build_lightning_query("passenger_state", key="P-1'; DROP", limit=1) == (
        "SELECT * FROM `passenger_state` WHERE \"KEY\" = 'P-1''; DROP' LIMIT 1"
    )


def test_builder_renders_timestamp_in_numbers_and_order():
    query = _build_lightning_query(
        "flight_impact",
        limit=200,
        where=[
            ("scheduled_time", ">=", datetime(2026, 9, 1, 0, 0, 0, 999)),
            ("delay_minutes", ">", 15),
            ("hotel_cost", "<=", Decimal("189.50")),
            ("status", "IN", ["DELAYED", "BOARDING"]),
            ("key", "LIKE", "RA4%"),
        ],
        order_by=("scheduled_time", "desc"),
    )
    assert query == (
        "SELECT * FROM `flight_impact` WHERE scheduled_time >= TIMESTAMP '2026-09-01 00:00:00'"
        " AND delay_minutes > 15 AND hotel_cost <= 189.50"
        " AND status IN ('DELAYED', 'BOARDING') AND \"KEY\" LIKE 'RA4%'"
        " ORDER BY scheduled_time DESC LIMIT 200"
    )


def test_builder_default_scan():
    assert _build_lightning_query("hotel_inventory") == "SELECT * FROM `hotel_inventory` LIMIT 10"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"topic": "flight_impact; DROP"},
        {"topic": "Flight_Impact"},
        {"topic": "flight_impact", "where": [("status OR 1=1", "=", "x")]},
        {"topic": "flight_impact", "where": [("status", "!=", "x")]},
        {"topic": "flight_impact", "where": [("status", "=", True)]},
        {"topic": "flight_impact", "where": [("status", "=", None)]},
        {"topic": "flight_impact", "where": [("status", "IN", "DELAYED")]},
        {"topic": "flight_impact", "where": [("status", "IN", [])]},
        {"topic": "flight_impact", "where": [("status", "=")]},
        {"topic": "flight_impact", "order_by": ("key", "SIDEWAYS")},
        {"topic": "flight_impact", "order_by": ("bad-column", "ASC")},
        {"topic": "flight_impact", "limit": 0},
        {"topic": "flight_impact", "limit": True},
    ],
)
def test_builder_rejects_invalid_input(kwargs):
    with pytest.raises(ValueError):
        _build_lightning_query(**kwargs)


# --- fake Lightning ----------------------------------------------------------


def _compare(row_value, op, value):
    if op == "IN":
        return row_value in value
    if isinstance(value, datetime):
        row_value = airport_app._timestamp(row_value)
    elif isinstance(value, (int, float, Decimal)):
        row_value = Decimal(str(row_value))
    if row_value is None:
        return False
    return {
        "=": row_value == value,
        ">": row_value > value,
        ">=": row_value >= value,
        "<": row_value < value,
        "<=": row_value <= value,
    }[op]


class FakeLightning:
    """In-memory tables that honour the builder's arguments and the 200-row cap."""

    def __init__(self, tables):
        self.tables = tables
        self.calls = []

    def query(self, topic, key=None, limit=200, where=(), order_by=None):
        _build_lightning_query(topic, key=key, limit=limit, where=where, order_by=order_by)
        self.calls.append({"topic": topic, "key": key, "where": list(where), "order_by": order_by})
        conditions = [("key", "=", key)] if key is not None else []
        conditions += list(where)
        rows = [
            dict(row)
            for row in self.tables.get(topic, [])
            if all(_compare(row.get(c), op, v) for c, op, v in conditions)
        ]
        if order_by:
            rows.sort(key=lambda row: row[order_by[0]], reverse=order_by[1] == "DESC")
        return rows[: min(limit, 200)]


@pytest.fixture
def fake(monkeypatch):
    lightning = FakeLightning({})
    monkeypatch.setattr(Deployment, "lightning_query", lambda _self, *args, **kwargs: lightning.query(*args, **kwargs))
    monkeypatch.setattr(airport_app, "_now", lambda: NOW)
    writes = []
    monkeypatch.setattr(airport_app, "_write_offer", lambda offer: writes.append(dict(offer)))
    lightning.writes = writes
    return lightning


def test_lightning_scan_pages_until_short_page(fake):
    fake.tables["passenger_state"] = [
        {"key": f"P-0928-417-{n:03d}", "inbound_flight_id": "RA417-20260928"} for n in range(1, 451)
    ]
    rows = airport_app.deployment.lightning_scan(
        "passenger_state", where=[("inbound_flight_id", "=", "RA417-20260928")]
    )
    assert len(rows) == 450
    assert len({row["key"] for row in rows}) == 450
    assert len(fake.calls) == 3
    assert fake.calls[0]["where"] == [("inbound_flight_id", "=", "RA417-20260928")]
    assert fake.calls[1]["where"][-1] == ("key", ">", "P-0928-417-200")
    assert fake.calls[2]["where"][-1] == ("key", ">", "P-0928-417-400")
    assert all(call["order_by"] == ("key", "ASC") for call in fake.calls)


def _offer(key, hotel_name, cost, status="OFFERED"):
    return {
        "key": key,
        "passenger_id": "P-0928-417-001",
        "recommended_flight_id": "RA88-20260928",
        "hotel_name": hotel_name,
        "status": status,
        "hotel_cost": cost,
        "impacted_at": "2026-09-28 10:00:00.000000",
        "recommended_at": "2026-09-28 10:01:00.000000",
    }


GRAND_HYATT = "Grand Hyatt at SFO"
MARRIOTT = "SFO Airport Marriott Waterfront"
HILTON = "Hilton SFO Airport Bayfront"
AS_OF = "2026-09-28 11:59:45"
HOTELS = [
    {"key": GRAND_HYATT, "available_rooms": "0", "nightly_rate": "329.00", "as_of": AS_OF},
    {"key": MARRIOTT, "available_rooms": "4", "nightly_rate": "229.00", "as_of": AS_OF},
    {"key": HILTON, "available_rooms": "30", "nightly_rate": "249.00", "as_of": AS_OF},
]


def _hotel_versions(updates):
    """Every update a hotel feed has sent, oldest first: Lightning keeps all of them, not just the latest."""
    return [
        {
            "key": name,
            "available_rooms": str(rooms),
            "nightly_rate": rate,
            "as_of": str(datetime(2026, 9, 28, 10, 0, 0) + timedelta(seconds=15 * n)),
        }
        for n in range(updates)
        for name, rooms, rate in (
            (GRAND_HYATT, max(0, 40 - n), "329.00"),
            (MARRIOTT, 150 + n % 4, "229.00"),
            (HILTON, 120, "249.00"),
        )
    ]


def test_hotels_show_each_hotels_latest_version_even_past_the_200_row_cap(fake):
    versions = _hotel_versions(100)  # 300 rows: more than Lightning returns in one query
    fake.tables["hotel_inventory"] = list(reversed(versions))  # arrival order must not matter
    hotels = airport_app._hotels()
    assert {name: row["available_rooms"] for name, row in hotels.items()} == {
        GRAND_HYATT: "0",
        MARRIOTT: "153",
        HILTON: "120",
    }
    assert fake.calls[-1]["order_by"] == ("as_of", "DESC")


def test_hotels_still_read_a_deployment_from_before_as_of_existed(monkeypatch):
    rows = [{"key": GRAND_HYATT, "available_rooms": "7", "nightly_rate": "329.00"}]

    def lightning_query(_self, topic, key=None, limit=200, where=(), order_by=None):
        if order_by:
            raise airport_app.HTTPException(status_code=502, detail="Column 'AS_OF' not found in any table")
        return rows

    monkeypatch.setattr(Deployment, "lightning_query", lightning_query)
    assert airport_app._hotels() == {GRAND_HYATT: rows[0]}


def test_selecting_a_sold_out_hotel_offers_a_third_hotel_then_book_closes_other(fake):
    _seed_offer_flights(fake)
    fake.tables["hotel_inventory"] = HOTELS
    fake.tables["passenger_recommendations"] = [
        _offer("P-0928-417-001-O1", GRAND_HYATT, "329.00"),
        _offer("P-0928-417-001-O2", MARRIOTT, "229.00"),
    ]
    replaced = airport_app.select_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert replaced["sold_out_hotel"] == GRAND_HYATT
    assert replaced["status"] == "OFFERED"  # not selected yet: the passenger chooses between Marriott and Hilton
    assert (replaced["hotel_name"], replaced["hotel_cost"]) == (HILTON, "249.00")

    fake.tables["passenger_recommendations"][0] = {k: v for k, v in replaced.items() if k != "sold_out_hotel"}
    chosen = airport_app.select_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert chosen["status"] == "SELECTED" and chosen["hotel_name"] == HILTON
    assert "sold_out_hotel" not in chosen

    fake.tables["passenger_recommendations"][0] = chosen
    booked = airport_app.book_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert booked["status"] == "BOOKED"
    assert [(w["key"], w["status"], w["hotel_name"]) for w in fake.writes] == [
        ("P-0928-417-001-O1", "OFFERED", HILTON),
        ("P-0928-417-001-O1", "SELECTED", HILTON),
        ("P-0928-417-001-O1", "BOOKED", HILTON),
        ("P-0928-417-001-O2", "CLOSED", MARRIOTT),
    ]


def test_sold_out_hotel_falls_back_to_the_other_offers_hotel_when_nothing_else_has_rooms(fake):
    _seed_offer_flights(fake)
    fake.tables["hotel_inventory"] = [
        {**row, "available_rooms": "0" if row["key"] != MARRIOTT else "4"} for row in HOTELS
    ]
    fake.tables["passenger_recommendations"] = [
        _offer("P-0928-417-001-O1", GRAND_HYATT, "329.00"),
        _offer("P-0928-417-001-O2", MARRIOTT, "229.00"),
    ]
    replaced = airport_app.select_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert replaced["hotel_name"] == MARRIOTT


def test_book_rejects_sold_out_hotel(fake):
    _seed_offer_flights(fake)
    fake.tables["hotel_inventory"] = HOTELS
    fake.tables["passenger_recommendations"] = [_offer("P-0928-417-001-O1", GRAND_HYATT, "329.00", status="SELECTED")]
    with pytest.raises(airport_app.HTTPException) as error:
        airport_app.book_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert error.value.status_code == 409


def test_passenger_returns_offers_and_hotel_options_cheapest_first(fake):
    _seed_offer_flights(fake)
    fake.tables["passenger_recommendations"] = [
        _offer("P-0928-417-001-O2", MARRIOTT, "229.00"),
        _offer("P-0928-417-001-O1", GRAND_HYATT, "329.00"),
    ]
    fake.tables["hotel_inventory"] = list(reversed(HOTELS))
    body = TestClient(airport_app.app).get("/api/passenger/P-0928-417-001").json()
    assert [row["key"] for row in body["offers"]] == ["P-0928-417-001-O1", "P-0928-417-001-O2"]
    assert body["hotels"] == [
        {"key": MARRIOTT, "available_rooms": 4, "nightly_rate": "229.00", "as_of": AS_OF},
        {"key": HILTON, "available_rooms": 30, "nightly_rate": "249.00", "as_of": AS_OF},
        {"key": GRAND_HYATT, "available_rooms": 0, "nightly_rate": "329.00", "as_of": AS_OF},
    ]


def test_select_and_book_offer_without_hotel(fake):
    _seed_offer_flights(fake)
    fake.tables["hotel_inventory"] = [{**row, "available_rooms": "0"} for row in HOTELS]
    fake.tables["passenger_recommendations"] = [_offer("P-0928-417-001-O1", None, None)]
    chosen = airport_app.select_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert chosen["status"] == "SELECTED"
    assert chosen["hotel_name"] is None and chosen["hotel_cost"] is None
    fake.tables["passenger_recommendations"][0] = chosen
    booked = airport_app.book_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert booked["status"] == "BOOKED"


def test_write_offer_passes_nulls_and_parses_timestamps(monkeypatch):
    published = []

    class FakePublisher:
        def publish(self, topic, key, value):
            published.append((topic, key, value))

        def flush(self):
            pass

    monkeypatch.setattr(airport_app, "_publisher", FakePublisher())
    offer = _offer("P-0928-417-001-O1", None, None)
    airport_app._write_offer(offer)
    offer.update(hotel_name=HILTON, hotel_cost="249.00", recommended_at=1790000000000)
    airport_app._write_offer(offer)
    (topic, key, first), (_, _, second) = published
    assert (topic, key) == ("passenger_recommendations", "P-0928-417-001-O1")
    assert first["hotel_name"] is None and first["hotel_cost"] is None
    assert first["impacted_at"] == datetime(2026, 9, 28, 10, 0)
    assert first["recommended_at"] == datetime(2026, 9, 28, 10, 1)
    assert set(first) == {
        "passenger_id",
        "recommended_flight_id",
        "hotel_name",
        "status",
        "hotel_cost",
        "impacted_at",
        "recommended_at",
    }
    assert second["hotel_cost"] == Decimal("249.00")
    assert second["recommended_at"] == datetime(2026, 9, 21, 14, 13, 20)


def _flight(n, scheduled, delay, affected, status="ON_TIME", origin="SFO", destination="SEA"):
    """The flight's flight_status row and its flight_impact row, as Lightning returns them."""
    row = {"key": f"RA{n}-20260928", "origin": origin, "destination": destination, "scheduled_time": scheduled}
    estimated = datetime.fromisoformat(scheduled) + timedelta(minutes=delay)
    return (
        {**row, "estimated_time": f"{estimated:%Y-%m-%d %H:%M:%S}.000000", "status": status},
        {**row, "status": status, "delay_minutes": str(delay), "affected_passengers": str(affected)},
    )


def _set_flights(fake, *flights):
    fake.tables["flight_status"] = [status for status, _ in flights]
    fake.tables["flight_impact"] = [impact for _, impact in flights]


def _seed_offer_flights(fake):
    _set_flights(
        fake,
        _flight(417, "2026-09-28 12:00:00", 60, 1, "DELAYED", origin="ORD", destination="SFO"),
        _flight(88, "2026-09-28 13:45:00", 0, 0),
    )
    fake.tables["passenger_state"] = [
        {"key": "P-0928-417-001", "inbound_flight_id": "RA417-20260928", "final_destination": "SEA", "risk": "HIGH"}
    ]


def test_recovery_rechecks_connection_at_selection_and_booking(fake):
    _seed_offer_flights(fake)
    passenger_id = "P-0928-417-001"
    key = passenger_id + "-O1"
    fake.tables["passenger_recommendations"] = [_offer(key, None, None)]
    client = TestClient(airport_app.app)
    path = f"/api/passenger/{passenger_id}"

    assert client.get(path).json()["offers"][0]["key"] == key  # exactly 45 minutes is eligible
    delayed = _flight(417, "2026-09-28 12:00:00", 61, 1, "DELAYED", origin="ORD", destination="SFO")
    fake.tables["flight_status"][0], fake.tables["flight_impact"][0] = delayed
    assert client.get(path).json()["offers"] == []
    assert client.post(f"{path}/select/{key}").status_code == 409
    fake.tables["passenger_recommendations"][0]["status"] = "SELECTED"
    assert client.post(f"{path}/book/{key}").status_code == 409
    assert fake.writes == []


def test_state_counts_and_delays(fake):
    _set_flights(
        fake,
        _flight(100, "2026-09-27 17:00:00.000000", 90, 50),  # before the 18h window
        _flight(417, "2026-09-28 11:00:00.000000", 95, 260, "DELAYED"),
        _flight(418, "2026-09-28 13:00:00.000000", 15, 3, "DELAYED"),
        _flight(419, "2026-09-28 09:00:00.000000", 14, 0),
        _flight(420, "2026-09-28 18:00:00.000000", 5, 0),  # at now+6h, excluded
        *(_flight(500 + n, f"2026-09-28 {n % 10:02d}:30:00.000000", 0, 1) for n in range(12)),
    )
    fake.tables["passenger_recommendations"] = [
        {"key": f"P-{n}-O1", "status": "BOOKED", "recommended_at": "2026-09-28 08:00:00.000000"} for n in range(205)
    ] + [
        {"key": "P-old-O1", "status": "BOOKED", "recommended_at": "2026-09-26 08:00:00.000000"},
        {"key": "P-open-O1", "status": "OFFERED", "recommended_at": "2026-09-28 08:00:00.000000"},
    ]
    body = TestClient(airport_app.app).get("/api/state").json()
    keys = [row["key"] for row in body["flights"]]
    assert "RA100-20260928" not in keys and "RA420-20260928" not in keys
    assert body["counts"] == {"affected": 275, "booked": 205, "flights": 15, "delayed": 2}
    assert body["flights"][0]["delay_minutes"] == 0 and isinstance(body["flights"][0]["affected_passengers"], int)
    # Most passengers at risk first, then the longest delay.
    assert [row["key"] for row in body["delays"][:3]] == ["RA417-20260928", "RA418-20260928", "RA500-20260928"]
    assert len(body["delays"]) == 10
    for topic in ("flight_status", "flight_impact"):
        assert next(call for call in fake.calls if call["topic"] == topic)["order_by"] == ("scheduled_time", "ASC")


def test_biggest_delays_puts_a_flight_with_passengers_at_risk_above_a_longer_delay(fake):
    _set_flights(
        fake,
        _flight(417, "2026-09-28 11:00:00.000000", 45, 95, "DELAYED"),
        _flight(155, "2026-09-28 10:00:00.000000", 235, 53, "DELAYED"),
        _flight(673, "2026-09-28 10:30:00.000000", 124, 0, "DELAYED"),
    )
    body = TestClient(airport_app.app).get("/api/state").json()
    assert [row["key"] for row in body["delays"]] == ["RA417-20260928", "RA155-20260928", "RA673-20260928"]


def test_flights_show_live_status_and_delay_with_flinks_at_risk_count(fake):
    status, impact = _flight(417, "2026-09-28 11:00:00.000000", 30, 7, "DELAYED", origin="ORD", destination="SFO")
    fake.tables["flight_status"] = [status]
    fake.tables["flight_impact"] = [{**impact, "status": "ON_TIME", "delay_minutes": "0"}]  # a minute behind
    body = TestClient(airport_app.app).get("/api/state").json()
    assert [(row["status"], row["delay_minutes"], row["affected_passengers"]) for row in body["flights"]] == [
        ("DELAYED", 30, 7)
    ]
    assert body["counts"]["delayed"] == 1


def _copy(status_row, delay, status):
    """Another flight_status row for the same flight, like the frozen copy Lightning can keep beside the live one."""
    estimated = datetime.fromisoformat(status_row["scheduled_time"]) + timedelta(minutes=delay)
    return {**status_row, "estimated_time": f"{estimated:%Y-%m-%d %H:%M:%S}.000000", "status": status}


def test_each_flight_shows_once_with_its_live_copy_past_the_200_row_cap(fake):
    start = NOW - airport_app.WINDOW_BEFORE
    flights = [_flight(n, f"{start + timedelta(minutes=8 * n):%Y-%m-%d %H:%M:%S}.000000", 0, 0) for n in range(180)]
    # flight: (frozen copy, live copy, flight_impact's delay), as on 2026-10-29's workshop run
    cases = {
        150: ((46, "DELAYED"), (115, "DELAYED"), 115),  # the delay grew
        151: ((227, "DELAYED"), (225, "DELAYED"), 225),  # an estimate came down
        152: ((20, "DELAYED"), (25, "LANDED"), 22),
    }  # Flink hasn't caught up with the landing yet
    frozen = []
    for n, ((old_delay, old_status), (delay, status), reported) in cases.items():
        live, impact = _flight(n, flights[n][0]["scheduled_time"], delay, 9, status)
        flights[n] = (live, {**impact, "delay_minutes": str(reported)})
        frozen.append(_copy(live, old_delay, old_status))
    frozen += [dict(live) for live, _ in flights[:32]]  # copies that never diverged
    _set_flights(fake, *flights)
    fake.tables["flight_status"] = frozen + fake.tables["flight_status"]  # 215 rows, the frozen copies first
    client = TestClient(airport_app.app)
    body = client.get("/api/state").json()
    assert [row["key"] for row in body["flights"]] == [live["key"] for live, _ in flights]
    assert body["counts"]["flights"] == 180
    shown = {row["key"]: (row["delay_minutes"], row["status"]) for row in body["flights"]}
    assert [shown[f"RA{n}-20260928"] for n in cases] == [(115, "DELAYED"), (225, "DELAYED"), (25, "LANDED")]
    assert client.get("/api/flight/RA150-20260928").json()["flight"]["delay_minutes"] == 115


def _passenger(n, inbound, connecting, risk):
    return {
        "key": f"P-0928-{n}",
        "inbound_flight_id": inbound,
        "inbound_seat": f"{n % 30 + 1}A",
        "connecting_flight_id": connecting,
        "connecting_seat": f"{n % 30 + 1}F" if connecting else None,
        "risk": risk,
    }


PASSENGERS = [
    _passenger(1, "RA417-20260928", "RA680-20260928", "OK"),
    _passenger(2, "RA417-20260928", "RA680-20260928", "HIGH"),
    _passenger(3, "RA417-20260928", None, "NO_CONNECTION"),
    _passenger(4, "RA417-20260928", "RA684-20260928", "HIGH"),
    _passenger(5, "RA418-20260928", "RA680-20260928", "HIGH"),
]


def test_flight_passengers_high_risk_first(fake):
    _set_flights(fake, _flight(417, "2026-09-28 11:00:00.000000", 95, 2, "DELAYED", origin="ORD", destination="SFO"))
    fake.tables["passenger_state"] = PASSENGERS
    client = TestClient(airport_app.app)
    body = client.get("/api/flight/RA417-20260928").json()
    assert body["flight"]["delay_minutes"] == 95
    assert body["flight"]["estimated_time"] == "2026-09-28T12:35:00"
    assert body["flight"]["arrival"] is True
    assert [(row["key"], row["seat"]) for row in body["passengers"]] == [
        ("P-0928-2", "3A"),
        ("P-0928-4", "5A"),
        ("P-0928-1", "2A"),
        ("P-0928-3", "4A"),
    ]
    assert body["seats"] == list(airport_app.SEATS)
    assert client.get("/api/flight/RA999-20260928").status_code == 404
    assert client.get("/api/passenger/P-missing").status_code == 404


def test_flight_and_passenger_choose_the_same_current_risk(fake, monkeypatch):
    monkeypatch.setattr(airport_app, "PAGE_SIZE", 2)
    _set_flights(
        fake,
        _flight(417, "2026-09-28 11:00:00", 40, 1, "DELAYED", origin="ORD", destination="SFO"),
        _flight(680, "2026-09-28 12:10:00", 0, 0),
    )
    passenger = _passenger(1, "RA417-20260928", "RA680-20260928", "OK")
    stale = {**passenger, "inbound_estimated_time": "2026-09-28 11:00:00", "connection_minutes": 70}
    live = {**passenger, "inbound_estimated_time": "2026-09-28 11:40:00", "connection_minutes": 30, "risk": "HIGH"}
    stale_onward = {**live, "connection_minutes": 55, "risk": "OK"}
    fake.tables["passenger_state"] = [stale_onward, live, stale]
    client = TestClient(airport_app.app)
    flight = client.get("/api/flight/RA417-20260928").json()
    assert [(p["key"], p["risk"], p["connection_minutes"]) for p in flight["passengers"]] == [
        (passenger["key"], "HIGH", 30)
    ]
    assert client.get(f"/api/passenger/{passenger['key']}").json()["passenger"] == live


def test_a_stale_passenger_state_copy_never_hides_a_high_risk_seat(fake):
    # Lightning can keep a frozen copy of a passenger_state row beside the live one; the live one has the
    # later inbound estimate, since delays only grow.
    _set_flights(fake, _flight(417, "2026-09-28 11:00:00.000000", 40, 1, "DELAYED", origin="ORD", destination="SFO"))
    live = {
        "key": "P-0928-417-001",
        "inbound_flight_id": "RA417-20260928",
        "inbound_seat": "3A",
        "connecting_flight_id": "RA681-20260928",
        "risk": "HIGH",
        "connection_minutes": "30",
        "inbound_estimated_time": "2026-09-28 11:40:00.000000",
    }
    stale = {**live, "risk": "OK", "connection_minutes": "60", "inbound_estimated_time": "2026-09-28 11:10:00.000000"}
    fake.tables["passenger_state"] = [live, stale]
    client = TestClient(airport_app.app)
    rows = client.get("/api/flight/RA417-20260928").json()["passengers"]
    assert [(row["key"], row["risk"]) for row in rows] == [("P-0928-417-001", "HIGH")]
    fake.tables["passenger_state"] = [stale, live]
    assert client.get("/api/passenger/P-0928-417-001").json()["passenger"]["risk"] == "HIGH"


def test_departure_shows_passengers_connecting_onto_it(fake):
    _set_flights(fake, _flight(680, "2026-09-28 12:30:00.000000", 0, 0, destination="PHX"))
    fake.tables["passenger_state"] = PASSENGERS
    body = TestClient(airport_app.app).get("/api/flight/RA680-20260928").json()
    assert body["flight"]["arrival"] is False
    assert [(row["key"], row["seat"]) for row in body["passengers"]] == [
        ("P-0928-2", "3F"),
        ("P-0928-5", "6F"),
        ("P-0928-1", "2F"),
    ]


def test_app_reads_lightning_settings_from_env(monkeypatch):
    monkeypatch.setattr(airport_app.rtce, "_find_credentials_file", lambda: pytest.fail("read a file"))
    for name, value in {
        "STREAMHOUSE_ENVIRONMENT_ID": "env-1",
        "STREAMHOUSE_CLUSTER_ID": "lkc-1",
        "STREAMHOUSE_REGION": "us-east-1",
        "CONFLUENT_RTCE_API_KEY": "k",
        "CONFLUENT_RTCE_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(name, value)
    assert Deployment().lightning_url == "https://sql.us-east-1.aws.confluent.cloud/query/v1alpha1"


# --- optional shared password ------------------------------------------------


def test_no_password_means_no_login(monkeypatch):
    monkeypatch.delenv("APP_PASSWORD", raising=False)
    assert TestClient(airport_app.app).get("/").status_code == 200


def test_password_required_when_set(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    client = TestClient(airport_app.app)
    login = client.get("/")
    assert login.status_code == 200 and 'action="/login"' in login.text and "<title>River Air sign in" in login.text
    assert client.get("/api/state").status_code == 401
    assert client.get("/", auth=("demo", "wrong")).status_code == 200  # wrong password: login page again
    assert "Wrong" not in client.get("/", auth=("demo", "wrong")).text
    assert client.get("/", auth=("demo", "wrong")).text == login.text
    assert client.get("/", auth=("demo", "s3cret")).status_code == 200
    assert client.get("/static/app.js").status_code == 401


def test_malformed_authorization_header_gets_401(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    client = TestClient(airport_app.app)
    for header in ["Basic !!!not-base64!!!", "Basic " + base64.b64encode(b"nocolonhere").decode(), "Bearer s3cret"]:
        response = client.get("/static/app.js", headers={"Authorization": header})
        assert response.status_code == 401, header
        assert response.headers["WWW-Authenticate"] == 'Basic realm="River Air"'


def _cookie_value(response):
    return response.cookies.get("river_air_session")


def test_login_sets_cookie_that_authorizes_everything(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    client = TestClient(airport_app.app, base_url="https://testserver")
    response = client.post("/login", data={"password": "s3cret"}, follow_redirects=False)
    assert response.status_code == 303 and response.headers["location"] == "/"
    header = response.headers["set-cookie"].lower()
    for attribute in ["httponly", "secure", "samesite=lax", "path=/", f"max-age={30 * 86400}"]:
        assert attribute in header
    assert "river_air_session=" in header
    assert client.get("/").text.lstrip().lower().startswith("<!doctype html>")
    assert 'action="/login"' not in client.get("/").text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/api/state").status_code != 401


def test_login_page_logo_is_public_but_other_static_is_not(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    client = TestClient(airport_app.app)
    assert client.get("/static/river-air-logo.svg").status_code == 200
    assert client.get("/static/app.js").status_code == 401


def test_api_without_sign_in_is_json_401(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    response = TestClient(airport_app.app).get("/api/state")
    assert response.status_code == 401
    assert response.json() == {"detail": "Sign in"}
    assert response.headers["WWW-Authenticate"] == 'Basic realm="River Air"'


def test_wrong_login_password_shows_error_without_cookie(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    response = TestClient(airport_app.app).post("/login", data={"password": "nope"}, follow_redirects=False)
    assert response.status_code == 401
    assert "Wrong password" in response.text and 'action="/login"' in response.text
    assert "set-cookie" not in response.headers


def test_cookie_from_old_password_is_rejected_after_rotation(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "old")
    client = TestClient(airport_app.app, base_url="https://testserver")
    client.post("/login", data={"password": "old"}, follow_redirects=False)
    assert client.get("/static/app.js").status_code == 200
    monkeypatch.setenv("APP_PASSWORD", "new")
    assert client.get("/static/app.js").status_code == 401
    assert 'action="/login"' in client.get("/").text


def test_logout_clears_cookie(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    client = TestClient(airport_app.app, base_url="https://testserver")
    client.post("/login", data={"password": "s3cret"}, follow_redirects=False)
    response = client.get("/logout", follow_redirects=False)
    assert response.status_code == 303 and response.headers["location"] == "/"
    assert "max-age=0" in response.headers["set-cookie"].lower()
    assert client.get("/static/app.js").status_code == 401


def test_no_password_mode_has_no_login_routes(monkeypatch):
    monkeypatch.delenv("APP_PASSWORD", raising=False)
    client = TestClient(airport_app.app)
    assert 'action="/login"' not in client.get("/").text
    assert client.get("/static/app.js").status_code == 200


def _reading(key, seconds_ago, **extra):
    as_of = (NOW - timedelta(seconds=seconds_ago)).strftime("%Y-%m-%d %H:%M:%S.%f")
    return {"key": key, "as_of": as_of, "stage": "CRUISING", "latitude": "40.1", **extra}


def test_state_returns_each_airborne_flights_latest_reading(fake):
    fake.tables["flight_telemetry"] = [_reading("RA417-20260928", 10), _reading("RA100-20260928", 300)]
    telemetry = airport_app.state()["telemetry"]
    assert list(telemetry) == ["RA417-20260928"]
    assert telemetry["RA417-20260928"]["stage"] == "CRUISING"
    assert telemetry["RA417-20260928"]["latitude"] == "40.1"


def test_state_keeps_the_newest_telemetry_copy(fake):
    fake.tables["flight_telemetry"] = [
        _reading("RA417-20260928", 30, latitude="1.0"),
        _reading("RA417-20260928", 5, latitude="2.0"),
    ]
    assert airport_app.state()["telemetry"]["RA417-20260928"]["latitude"] == "2.0"


def test_state_pages_telemetry_past_the_200_row_cap(fake):
    fake.tables["flight_telemetry"] = [_reading(f"RA{n:03d}-20260928", 5) for n in range(250)]
    assert len(airport_app.state()["telemetry"]) == 250


def test_state_survives_a_failed_telemetry_read(fake, monkeypatch):
    fake.tables["flight_status"] = [
        {
            "key": "RA417-20260928",
            "scheduled_time": "2026-09-28 11:00:00.000",
            "estimated_time": "2026-09-28 11:00:00.000",
        }
    ]
    healthy = fake.query

    def query(topic, *args, **kwargs):
        if topic == "flight_telemetry":
            raise airport_app.HTTPException(status_code=502, detail="no Lightning Table")
        return healthy(topic, *args, **kwargs)

    monkeypatch.setattr(Deployment, "lightning_query", lambda _self, *args, **kwargs: query(*args, **kwargs))
    result = airport_app.state()
    assert [flight["key"] for flight in result["flights"]] == ["RA417-20260928"]
    assert result["telemetry"] == {}
    assert "flight_telemetry" in result["notice"]


def test_state_has_no_notice_when_telemetry_reads(fake):
    fake.tables["flight_status"] = [
        {
            "key": "RA417-20260928",
            "scheduled_time": "2026-09-28 11:00:00.000",
            "estimated_time": "2026-09-28 11:00:00.000",
        }
    ]
    fake.tables["flight_telemetry"] = [_reading("RA417-20260928", 10)]
    assert airport_app.state()["notice"] is None


def test_state_tells_an_empty_dashboard_to_start_the_datagen(fake):
    assert "uv run airport-datagen" in airport_app.state()["notice"]


@pytest.mark.parametrize(
    "body",
    [
        '{"errors":[{"code":"FE_CUSTOMER_NOT_FOUND"}]}',
        '{"errors":[{"code":"FE_TABLE_NOT_FOUND","detail":"Table does not exist: \'passenger_state\'"}]}',
    ],
)
def test_a_topic_without_a_lightning_table_names_the_workshop_step(body):
    message = airport_app._lightning_error("passenger_state", 400, body)
    assert "passenger_state has no Lightning Table yet" in message
    assert "Lab 3" in message and "uv run workshop-status" in message


def test_other_lightning_errors_keep_the_status_and_body():
    assert airport_app._lightning_error("hotel_inventory", 500, "boom") == "Lightning query returned HTTP 500: boom"


def test_a_topic_with_no_data_yet_reads_as_empty(monkeypatch):
    deployment = Deployment()
    deployment._infra = {"region": "us-east-1", "env_id": "env-1", "cluster_id": "lkc-1"}
    deployment._rtce_key, deployment._rtce_secret = "key", "secret"
    unavailable = '{"errors":[{"code":"DP_TABLE_NOT_AVAILABLE","detail":"Table data is not yet available."}]}'
    monkeypatch.setattr(
        airport_app.requests, "post", lambda *args, **kwargs: SimpleNamespace(status_code=400, text=unavailable)
    )
    assert deployment.lightning_query("flight_status") == []
