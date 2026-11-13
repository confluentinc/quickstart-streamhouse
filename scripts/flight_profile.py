"""The flight profile behind the live map: where a flight is and how it is flying.

A Python port of the profile in static/flightmap.js (routeGeometry, pointAlong,
planePosition and the altitude and vertical speed of telemetry), so the generator
can stream the same readings the browser used to simulate. Each plane takes off at
RUNWAY_KT, speeds up to cruise over CLIMB_MIN, cruises, and slows back to RUNWAY_KT
over the last DESCENT_MIN. A late arrival circles at the top of its descent until it
can land at its estimated time. Keep the numbers in step with flightmap.js;
tests/test_flight_profile.py compares them against a fixture captured from it.
"""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import cache

HUB = "SFO"
CRUISE_KMH = 850  # typical narrow-body cruise speed
GROUND_OVERHEAD_MIN = 25  # taxi + climb/descent, roughly constant regardless of distance

AIRPORTS: dict[str, tuple[float, float]] = {
    "SFO": (37.6, -122.4), "ORD": (41.9, -87.9), "SEA": (47.4, -122.3), "LAX": (33.9, -118.4),
    "DEN": (39.9, -104.7), "PHX": (33.4, -112.0), "DFW": (32.9, -97.0), "ATL": (33.6, -84.4),
    "JFK": (40.6, -73.8), "BOS": (42.4, -71.0), "MSP": (44.9, -93.2), "LAS": (36.1, -115.2),
    "SAN": (32.7, -117.2), "PDX": (45.6, -122.6), "SLC": (40.8, -111.9), "IAH": (30.0, -95.3),
}  # fmt: skip

BOUNDS = {"lonMin": -125.0, "lonMax": -66.0, "latMin": 24.0, "latMax": 49.0}
MAP_W = 900
MAP_H = 484
ROUTE_BULGE = 0.14  # how far the route curve bows toward the top of the map, per unit of length

CLIMB_MIN = 20  # takeoff to cruise
DESCENT_MIN = 25  # top of descent to landing
RUNWAY_KT = 160  # ground speed at takeoff and touchdown
HOLD_LAP_MIN = 4  # about one lap of the circle a late arrival flies before its descent
ROUTE_SAMPLES = 48  # points along each route curve, for measuring distance on it
KM_PER_NM = 1.852
NM_PX = MAP_H / (BOUNDS["latMax"] - BOUNDS["latMin"]) / 60  # map units per nautical mile
GUST_NM, GUST_S = 0.02, 13  # each plane surges +-GUST_NM along its path
SWAY_FT, SWAY_S = 20, 10  # at cruise the altitude drifts +-SWAY_FT
CRUISE_LEVELS_FT = (33000, 35000, 37000, 39000)


@dataclass(frozen=True)
class Reading:
    stage: str  # CLIMBING, CRUISING, HOLDING, DESCENDING or LANDED
    latitude: float
    longitude: float
    altitude_ft: int
    ground_speed_kt: float
    vertical_speed_fpm: int
    heading_deg: float
    distance_to_go_nm: float
    hold_ends_at: datetime | None


@dataclass(frozen=True)
class _Route:
    x0: float
    y0: float
    cx: float
    cy: float
    x1: float
    y1: float
    nm: tuple[float, ...]  # ground distance from the origin to each sampled point

    @property
    def length_nm(self) -> float:
        return self.nm[-1]


def flight_seed(key: str) -> float:
    """A steady 0..1 value per flight, the same 32-bit hash the browser used."""
    value = 0
    for char in key:
        value = (value * 31 + ord(char)) & 0xFFFFFFFF
    return value / 2**32


def _project(lat: float, lon: float) -> tuple[float, float]:
    x = (lon - BOUNDS["lonMin"]) / (BOUNDS["lonMax"] - BOUNDS["lonMin"]) * MAP_W
    y = (BOUNDS["latMax"] - lat) / (BOUNDS["latMax"] - BOUNDS["latMin"]) * MAP_H
    return x, y


def _unproject(x: float, y: float) -> tuple[float, float]:
    return (
        BOUNDS["latMax"] - y / MAP_H * (BOUNDS["latMax"] - BOUNDS["latMin"]),
        BOUNDS["lonMin"] + x / MAP_W * (BOUNDS["lonMax"] - BOUNDS["lonMin"]),
    )


def _haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    (lat1, lon1), (lat2, lon2) = a, b
    rad = math.pi / 180
    d_lat, d_lon = (lat2 - lat1) * rad, (lon2 - lon1) * rad
    h = math.sin(d_lat / 2) ** 2 + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(d_lon / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(h))


def flight_duration(origin: str, destination: str) -> timedelta:
    """Gate to gate: the distance at cruise speed plus the ground overhead."""
    hours = _haversine_km(AIRPORTS[origin], AIRPORTS[destination]) / CRUISE_KMH + GROUND_OVERHEAD_MIN / 60
    return timedelta(hours=hours)


def _bezier_point(r: _Route, t: float) -> tuple[float, float]:
    mt = 1 - t
    return (
        mt * mt * r.x0 + 2 * mt * t * r.cx + t * t * r.x1,
        mt * mt * r.y0 + 2 * mt * t * r.cy + t * t * r.y1,
    )


def _bezier_angle_deg(r: _Route, t: float) -> float:
    mt = 1 - t
    dx = 2 * mt * (r.cx - r.x0) + 2 * t * (r.x1 - r.cx)
    dy = 2 * mt * (r.cy - r.y0) + 2 * t * (r.y1 - r.cy)
    return math.atan2(dy, dx) * (180 / math.pi)


@cache
def _route_geometry(origin: str, destination: str) -> _Route:
    x0, y0 = _project(*AIRPORTS[origin])
    x1, y1 = _project(*AIRPORTS[destination])
    dx, dy = x1 - x0, y1 - y0
    length = math.hypot(dx, dy) or 1
    px, py = -dy / length, dx / length
    if py > 0:  # always bow "up" regardless of travel direction
        px, py = -px, -py
    bulge = length * ROUTE_BULGE
    route = _Route(x0, y0, (x0 + x1) / 2 + px * bulge, (y0 + y1) / 2 + py * bulge, x1, y1, ())
    points = [_unproject(*_bezier_point(route, i / ROUTE_SAMPLES)) for i in range(ROUTE_SAMPLES + 1)]
    nm = [0.0]
    for i in range(1, ROUTE_SAMPLES + 1):
        nm.append(nm[i - 1] + _haversine_km(points[i - 1], points[i]) / KM_PER_NM)
    return _Route(x0, y0, route.cx, route.cy, x1, y1, tuple(nm))


def _point_along(route: _Route, along_nm: float) -> tuple[float, float, float]:
    """The point `along_nm` from the origin (x, y) and the direction of travel there."""
    nm = route.nm
    i = 1
    while i < ROUTE_SAMPLES and nm[i] < along_nm:
        i += 1
    t = max(0.0, min(1.0, (i - 1 + (along_nm - nm[i - 1]) / (nm[i] - nm[i - 1])) / ROUTE_SAMPLES))
    x, y = _bezier_point(route, t)
    return x, y, _bezier_angle_deg(route, t)


def _js_round(value: float) -> int:
    return math.floor(value + 0.5)


def reading(
    key: str,
    origin: str,
    destination: str,
    scheduled: datetime,
    estimated: datetime,
    status: str,
    at: datetime,
) -> Reading | None:
    """Where the flight is and how it is flying at `at` (all datetimes naive UTC).

    `scheduled` and `estimated` are the flight's one real timestamp, as scheduled and
    as now expected: the arrival at SFO, or the departure from it. None while the
    flight is at the gate; a LANDED reading at the destination once it has touched down.
    """
    duration = flight_duration(origin, destination)
    is_arrival = destination == HUB

    def seconds(moment: datetime) -> float:
        return moment.replace(tzinfo=timezone.utc).timestamp()

    now = seconds(at)
    if is_arrival:
        estimated_arrival = seconds(estimated)
        departure = min(seconds(scheduled), estimated_arrival) - duration.total_seconds()
    else:
        departure = seconds(estimated)
        estimated_arrival = departure + duration.total_seconds()

    landed = (is_arrival and status == "LANDED") or now >= estimated_arrival
    if landed:
        latitude, longitude = AIRPORTS[destination]
        return Reading("LANDED", latitude, longitude, 0, 0.0, 0, 0.0, 0.0, None)
    if now < departure or (not is_arrival and status != "DEPARTED"):
        return None

    route = _route_geometry(origin, destination)
    flight_min = duration.total_seconds() / 60
    total_min = (estimated_arrival - departure) / 60
    minutes = max(0.0, min(total_min, (now - departure) / 60))
    to_land = total_min - minutes
    # The cruise speed that covers the route in flight_min around the slower climb and descent.
    ramps = (CLIMB_MIN + DESCENT_MIN) / 2
    cruise_kt = (route.length_nm * 60 - RUNWAY_KT * ramps) / (flight_min - ramps)

    def ramp_nm(m: float, ramp_min: float) -> float:
        return (RUNWAY_KT * m + ((cruise_kt - RUNWAY_KT) * m * m) / (2 * ramp_min)) / 60

    def ramp_kt(m: float, ramp_min: float) -> float:
        return RUNWAY_KT + ((cruise_kt - RUNWAY_KT) * m) / ramp_min

    hold_nm = cruise_kt * (total_min - flight_min) / 60
    if minutes < CLIMB_MIN:
        stage = "CLIMBING"
        flown_nm, knots = ramp_nm(minutes, CLIMB_MIN), ramp_kt(minutes, CLIMB_MIN)
    elif to_land < DESCENT_MIN:
        stage = "DESCENDING"
        flown_nm = route.length_nm + hold_nm - ramp_nm(to_land, DESCENT_MIN)
        knots = ramp_kt(to_land, DESCENT_MIN)
    else:
        stage = "CRUISING"
        flown_nm = ramp_nm(CLIMB_MIN, CLIMB_MIN) + cruise_kt * (minutes - CLIMB_MIN) / 60
        knots = cruise_kt
    level = max(0.0, min(1.0, minutes / CLIMB_MIN, to_land / DESCENT_MIN))  # share of cruise altitude
    seed = flight_seed(key)
    gust = now / GUST_S + seed * 2 * math.pi
    flown_nm += GUST_NM * math.sin(gust) * level
    knots += (GUST_NM / GUST_S) * 3600 * math.cos(gust) * level

    # The hold: whole laps, turning right as holds are flown, from the top of
    # descent, so the plane leaves the circle where it joined it.
    hold_at_nm = route.length_nm - ramp_nm(DESCENT_MIN, DESCENT_MIN)
    holding = hold_at_nm < flown_nm < hold_at_nm + hold_nm
    if holding:
        along_nm = hold_at_nm
    elif flown_nm > hold_at_nm:
        along_nm = flown_nm - hold_nm
    else:
        along_nm = flown_nm
    x, y, angle_deg = _point_along(route, along_nm)
    if holding:
        laps = max(1, _js_round((total_min - flight_min) / HOLD_LAP_MIN))
        radius = hold_nm / laps / (2 * math.pi)
        entry = angle_deg * math.pi / 180
        heading = entry + (flown_nm - hold_at_nm) / radius
        x += radius * NM_PX * (math.sin(heading) - math.sin(entry))
        y += radius * NM_PX * (math.cos(entry) - math.cos(heading))
        angle_deg = ((heading * 180 / math.pi) % 360 + 540) % 360 - 180
        stage = "HOLDING"

    cruise_ft = CRUISE_LEVELS_FT[math.floor(seed * 4)]
    sway = now / SWAY_S + seed * 2 * math.pi
    cruising = stage in ("CRUISING", "HOLDING")
    altitude = cruise_ft * level + (SWAY_FT * math.sin(sway) if cruising else 0)
    if cruising:  # the sway's own rate
        climb_rate = SWAY_FT / SWAY_S * 60 * math.cos(sway)
    elif stage == "CLIMBING":
        climb_rate = cruise_ft / CLIMB_MIN
    else:
        climb_rate = -cruise_ft / DESCENT_MIN
    latitude, longitude = _unproject(x, y)
    return Reading(
        stage=stage,
        latitude=latitude,
        longitude=longitude,
        altitude_ft=_js_round(altitude),
        ground_speed_kt=knots,
        vertical_speed_fpm=_js_round(climb_rate),
        heading_deg=(angle_deg + 90) % 360,
        distance_to_go_nm=route.length_nm - along_nm,
        hold_ends_at=estimated - timedelta(minutes=DESCENT_MIN) if holding else None,
    )
