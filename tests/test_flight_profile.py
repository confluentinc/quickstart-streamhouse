import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts import flight_profile as fp

GOLDEN = json.loads(Path("tests/fixtures/flight-profile-golden.json").read_text())
FLIGHTMAP_JS = Path("scripts/static/flightmap.js").read_text()
STAGES = {"climb": "CLIMBING", "cruise": "CRUISING", "hold": "HOLDING", "descent": "DESCENDING"}


def test_the_golden_fixture_covers_every_stage():
    assert len(GOLDEN) >= 100
    assert {s["stage"] for s in GOLDEN if s["phase"] == "enroute"} == set(STAGES)


@pytest.mark.parametrize(
    "sample", GOLDEN, ids=lambda s: f"{s['inputs']['key']}-{s['inputs']['status']}-{s['inputs']['at']}"
)
def test_readings_match_todays_browser_profile(sample):
    i = sample["inputs"]
    at = datetime.fromisoformat(i["at"])
    at_ms = at.replace(tzinfo=timezone.utc).timestamp() * 1000
    got = fp.reading(
        i["key"],
        i["origin"],
        i["destination"],
        datetime.fromisoformat(i["scheduled"]),
        datetime.fromisoformat(i["estimated"]),
        i["status"],
        at,
    )
    if sample["phase"] == "done" or at_ms >= sample["estimated_arrival_ms"]:
        assert got is not None and got.stage == "LANDED"
        assert (got.latitude, got.longitude) == fp.AIRPORTS[i["destination"]]
        assert (got.altitude_ft, got.ground_speed_kt, got.vertical_speed_fpm) == (0, 0, 0)
        assert got.distance_to_go_nm == 0
        return
    if sample["phase"] == "waiting" or at_ms < sample["departure_ms"]:
        assert got is None
        return
    assert got.stage == STAGES[sample["stage"]]
    assert abs(got.latitude - sample["lat"]) < 1e-6
    assert abs(got.longitude - sample["lon"]) < 1e-6
    assert abs(got.ground_speed_kt - sample["knots"]) < 1e-6
    assert abs(got.distance_to_go_nm - sample["to_go_nm"]) < 1e-6
    assert abs(got.heading_deg - ((sample["angle_deg"] + 90) % 360)) < 1e-6
    assert abs(got.altitude_ft - sample["altitude_ft"]) <= 1
    assert abs(got.vertical_speed_fpm - sample["vertical_speed_fpm"]) <= 1
    if got.stage == "HOLDING":
        assert got.hold_ends_at == datetime.fromisoformat(i["estimated"]) - timedelta(minutes=fp.DESCENT_MIN)
    else:
        assert got.hold_ends_at is None


def test_flight_seed_matches_the_browser_hash():
    for sample in GOLDEN:
        assert fp.flight_seed(sample["inputs"]["key"]) == sample["flight_seed"]


def test_map_constants_match_flightmap_js():
    block = re.search(r"const AIRPORTS = \{(.*?)\};", FLIGHTMAP_JS, re.S).group(1)
    airports = {
        code: (float(lat), float(lon)) for code, lat, lon in re.findall(r"(\w{3}): \[(-?[\d.]+), (-?[\d.]+)\]", block)
    }
    assert fp.AIRPORTS == airports
    bounds = dict(re.findall(r"(\w+): (-?\d+)", re.search(r"const BOUNDS = \{(.*?)\}", FLIGHTMAP_JS).group(1)))
    assert fp.BOUNDS == {k: float(v) for k, v in bounds.items()}
    assert re.search(r"const MAP_W = (\d+), MAP_H = (\d+);", FLIGHTMAP_JS).groups() == (str(fp.MAP_W), str(fp.MAP_H))
    assert int(re.search(r"const CRUISE_KMH = (\d+);", FLIGHTMAP_JS).group(1)) == fp.CRUISE_KMH
    assert int(re.search(r"const GROUND_OVERHEAD_MIN = (\d+);", FLIGHTMAP_JS).group(1)) == fp.GROUND_OVERHEAD_MIN
    assert float(re.search(r"const bulge = len \* ([\d.]+);", FLIGHTMAP_JS).group(1)) == fp.ROUTE_BULGE


def test_an_early_arrival_touches_down_at_its_estimated_time():
    scheduled, estimated = datetime(2026, 11, 12, 12, 0), datetime(2026, 11, 12, 11, 52)
    args = ("RA150-20261112", "LAS", "SFO", scheduled, estimated, "ON_TIME")
    assert fp.reading(*args, datetime(2026, 11, 12, 11, 51, 59)).stage == "DESCENDING"
    landed = fp.reading(*args, datetime(2026, 11, 12, 11, 52, 0))
    assert landed.stage == "LANDED"
    assert (landed.latitude, landed.longitude) == fp.AIRPORTS["SFO"]


def test_flight_duration_is_distance_over_cruise_speed_plus_ground_overhead():
    assert fp.flight_duration("ORD", "SFO").total_seconds() == pytest.approx(
        (GOLDEN[0]["estimated_arrival_ms"] - GOLDEN[0]["departure_ms"]) / 1000
    )
