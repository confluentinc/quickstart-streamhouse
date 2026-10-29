"""River Air flight recovery data: 30 days of history plus one live day at the SFO hub.

Each service day has 180 flights: 90 arrivals into SFO and 90 departures from it. Every
flight uses one 182-seat layout, modeled after a two-class Boeing 737-900ER, and each
passenger has a seat on each of their flights.
The live day is shifted so flight RA417 from Chicago is scheduled 50 minutes after the
stream starts. Its delay grows every 30 seconds from +5 to +25 minutes, and 150 of its 170
passengers miss their connection. Every active flight publishes its status once a minute
(RA417 every 30 seconds), en-route arrivals revise their estimate by a minute or two along
the way, and both hotels publish their rooms every 15 seconds. Announced delays only grow
(a severe delay may shrink by at most three minutes near landing), so a flight never goes
from hours late back to on time.

The generator writes two rebooking offers a few seconds after a connection becomes HIGH
risk. Past bookings are completed the way customers would complete them; RA417's offers
stay open for the presenter. When the recovery agent runs in Flink (sql/26-31), it writes
RA417's offers and the generator writes every other flight's.

    uv run airport-datagen                  # history + live day, then stream 90 minutes
    uv run airport-datagen --offers generator  # write RA417's offers even if the agent runs
    uv run airport-datagen --speed 5        # play the 90-minute stream in 18 minutes
    uv run airport-datagen --minutes 0      # publish the current state and exit
    uv run airport-datagen --resume         # pick a stopped stream back up on its original clock
    uv run airport-datagen --dry-run --skip-history --minutes 5
    uv run airport-datagen --reset          # tombstone every generated key
"""

import argparse
import json
import logging
import os
import random
import signal
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from itertools import pairwise

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from .logging_utils import setup_logging
from .terraform import extract_kafka_credentials, get_project_root

FLIGHTS = "flight_status"
ITINERARIES = "passenger_itineraries"
HOTELS = "hotel_inventory"  # fed by the hotel webhook when deploy created one
WEBHOOK_ATTEMPTS = 5
OFFERS = "passenger_recommendations"

AIRLINE = "RA"
HUB = "SFO"
SPOKES = ("ORD", "SEA", "LAX", "DEN", "PHX", "DFW", "ATL", "JFK", "BOS", "MSP", "LAS", "SAN", "PDX", "SLC", "IAH")

# Daily template, in minutes after midnight. 90 arrivals from 06:00 to 23:00; 75
# departures from 06:30 to 22:00 plus a last bank of 15, one to each destination.
ARRIVAL_MINUTES = tuple(360 + round(i * 1020 / 89) for i in range(90))
DEPARTURE_MINUTES = (
    *(390 + round(i * 930 / 74) for i in range(75)),
    1330, 1335, 1340, 1345, 1350, 1356, 1362, 1368, 1374, 1380, 1386, 1392, 1398, 1404, 1410)
LAST_BANK = 75

# Every flight's seat map, modeled after a two-class Boeing 737-900ER: First in rows 1-5
# (A C | D F) and Economy in rows 6-32 (A B C | D E F), 182 seats.
SEATS = tuple(f"{row}{letter}" for row in range(1, 33) for letter in ("ACDF" if row <= 5 else "ABCDEF"))

HERO_NUMBER = 417
HERO_ORIGIN = "ORD"
HERO_MINUTE = 21 * 60 + 40
HERO_LEAD = timedelta(minutes=50)
# From +5 to +25 minutes into the stream, each 30-second RA417 update adds to its delay, up to 115.
HERO_EVERY = timedelta(seconds=30)
HERO_RAMP = (timedelta(minutes=5), timedelta(minutes=25))
HERO_DELAY = 115
HERO_CONNECTING = 150
HERO_LOCAL = 20
HERO_BUFFER = (50, 110)

CONNECTION_MINUTES = 45  # Flink's HIGH threshold in sql/24-serving-passenger-state.sql
CONNECTING_SHARE = 0.3
CONNECTION_BUFFER = (60, 180)
PASSENGERS_PER_FLIGHT = (120, 180)
SEVERE_PER_DAY = 10
SEVERE_DELAY = (90, 240)
MODERATE_SHARE = 0.25
MODERATE_DELAY = (15, 60)
DELAYED_AT = 15
MAP_AT_RISK = 45  # the dashboard's flight map colors a flight this many minutes late as at risk
ACTIVE_BEFORE = timedelta(minutes=180)
BOARDING = timedelta(minutes=30)
# Live-stream arrival-time revisions: chance per update, minutes off the announced delay, and when they stop.
REVISION_CHANCE = 0.8
REVISION_MINUTES = (-1, 0, 1, 2)
SETTLE_BEFORE = timedelta(minutes=10)

GRAND_HYATT = "Grand Hyatt at SFO"
MARRIOTT = "SFO Airport Marriott Waterfront"
HILTON = "Hilton SFO Airport Bayfront"
HOTEL_ORDER = (GRAND_HYATT, MARRIOTT, HILTON)  # offered in this order; each offer takes the next hotel with rooms
HOTEL_RATES = {GRAND_HYATT: Decimal("329.00"), MARRIOTT: Decimal("229.00"), HILTON: Decimal("249.00")}
GRAND_HYATT_ROOMS = 40
MARRIOTT_ROOMS = 150
HILTON_ROOMS = 120
SELLOUT = timedelta(minutes=40)  # the Grand Hyatt sells out this long after the stream starts
HOTEL_EVERY = timedelta(seconds=15)  # both hotels publish their rooms this often during the stream
OVERNIGHT = timedelta(hours=6)
AGENT_LATENCY_SECONDS = (5, 40)
BOOKING_SHARE = 0.85
DECISION_SECONDS = (3 * 60, 90 * 60)
OFFER_EXPIRY = timedelta(minutes=90)

HISTORY_DAYS = 30
STREAM_MINUTES = 90
SEED = 42


@dataclass
class Flight:
    key: str
    number: int
    origin: str
    destination: str
    scheduled: datetime
    offset: int = 0  # seconds after each minute this flight publishes
    reveals: list[tuple[datetime, int]] = field(default_factory=list)  # (published at, delay minutes)
    every: timedelta = timedelta(minutes=1)  # how often it publishes while active

    @property
    def arrival(self) -> bool:
        return self.destination == HUB

    def delay_at(self, at: datetime) -> int:
        delay = 0
        for when, minutes in self.reveals:
            if when <= at:
                delay = minutes
        return delay

    def estimated_at(self, at: datetime = datetime.max) -> datetime:
        return self.scheduled + timedelta(minutes=self.delay_at(at))

    def status_at(self, at: datetime) -> str:
        estimated = self.estimated_at(at)
        if at >= estimated:
            return "LANDED" if self.arrival else "DEPARTED"
        if not self.arrival and at >= estimated - BOARDING:
            return "BOARDING"
        return "DELAYED" if self.delay_at(at) >= DELAYED_AT else "ON_TIME"

    def row(self, at: datetime) -> dict:
        return dict(origin=self.origin, destination=self.destination, scheduled_time=self.scheduled,
                    estimated_time=self.estimated_at(at), status=self.status_at(at))

    def ticks(self, after: datetime, until: datetime):
        """Update times in (after, until], one per `every`, from three hours out to completion."""
        tick = self.scheduled - ACTIVE_BEFORE + timedelta(seconds=self.offset)
        last = self.estimated_at() + timedelta(seconds=self.offset)
        if tick <= after:
            tick += self.every * ((after - tick) // self.every + 1)
        while tick <= min(last, until):
            yield tick
            tick += self.every


@dataclass
class Passenger:
    key: str
    inbound: Flight
    inbound_seat: str
    connecting: Flight | None = None
    connecting_seat: str | None = None
    impacted_at: datetime | None = None


@dataclass
class Recovery:
    """The agent's two offers for one HIGH-risk passenger and what the passenger did with them."""

    passenger: Passenger
    options: list[Flight]
    hotels: list[str | None]
    recommended_at: datetime
    resolved_at: datetime
    booked: int | None  # index of the booked option; None if the passenger never chose
    completes: bool = True  # False leaves the offers open for the presenter

    def rows(self, at: datetime):
        if at < self.recommended_at:
            return
        final = self.completes and at >= self.resolved_at
        for index, (flight, hotel) in enumerate(zip(self.options, self.hotels, strict=True)):
            status = "OFFERED"
            if final:
                status = "BOOKED" if index == self.booked else "CLOSED"
            yield f"{self.passenger.key}-O{index + 1}", dict(
                passenger_id=self.passenger.key, recommended_flight_id=flight.key, hotel_name=hotel,
                status=status, hotel_cost=HOTEL_RATES[hotel] if hotel else None,
                impacted_at=self.passenger.impacted_at, recommended_at=self.recommended_at)


@dataclass
class Day:
    service_date: date
    live: bool
    arrivals: list[Flight]
    departures: list[Flight]
    passengers: list[Passenger]

    @property
    def flights(self) -> list[Flight]:
        return self.arrivals + self.departures

    @property
    def hero(self) -> Flight:
        return self.arrivals[ARRIVAL_MINUTES.index(HERO_MINUTE)]


@dataclass
class Plan:
    start: datetime
    days: list[Day]  # oldest history first, live day last
    tomorrow: Day
    recoveries: list[Recovery]
    seed: int = SEED

    @property
    def live(self) -> Day:
        return self.days[-1]


def _clock(value: str | None) -> datetime:
    """Stream start as naive UTC, trimmed to the minute."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")) if value else datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)


def _delays(flight: Flight, steps) -> None:
    """Announce delays at (minutes from schedule, delay) steps, on this flight's update tick."""
    flight.reveals = [(flight.scheduled + timedelta(minutes=rel, seconds=flight.offset), delay)
                      for rel, delay in steps]


def _assign_delays(day: Day, rng: random.Random) -> None:
    hero = day.hero if day.live else None
    forced = {f.key for f in day.departures[LAST_BANK:]} if day.live else set()
    pool = [f for f in day.flights if f is not hero and f.key not in forced]
    severe = {f.key for f in rng.sample(pool, SEVERE_PER_DAY - (1 if hero else 0))}
    for flight in day.flights:
        if flight is hero:
            start, (first, last) = flight.scheduled - HERO_LEAD, HERO_RAMP
            steps = (last - first) // HERO_EVERY
            flight.every = HERO_EVERY
            flight.reveals = [(start + first + HERO_EVERY * k, round(HERO_DELAY * k / steps))
                              for k in range(1, steps + 1)]
        elif flight.key in forced:
            continue
        elif flight.key in severe:
            delay = rng.randint(*SEVERE_DELAY)
            steps = [(-120, round(delay * 0.3)), (-80, round(delay * 0.65)), (-40, delay)]
            makeup = rng.randint(0, 3) if flight.arrival else 0
            if makeup:
                steps.append((delay - 20, delay - makeup))
            _delays(flight, steps)
        elif rng.random() < MODERATE_SHARE:
            delay = rng.randint(*MODERATE_DELAY)
            _delays(flight, [(-90, delay // 2), (-45, delay)])
        else:
            delay = rng.randint(-8 if flight.arrival else 0, 12)
            if delay:
                _delays(flight, [(-30 if flight.arrival else -40, delay)])


def _risk_flip(inbound: Flight, onward: Flight) -> tuple[bool, datetime | None]:
    """Whether the connection's risk changes at most once (OK -> HIGH), and when it does."""
    def at_risk(at):
        return onward.estimated_at(at) - inbound.estimated_at(at) < timedelta(minutes=CONNECTION_MINUTES)

    if at_risk(datetime.min):
        return False, None
    flip = None
    for when in sorted({w for w, _ in inbound.reveals} | {w for w, _ in onward.reveals}):
        risky = at_risk(when)
        if flip is None and risky:
            flip = when
        elif flip is not None and not risky:
            return False, None
    return True, flip


def _passengers(day: Day, rng: random.Random, seat_rng: random.Random) -> list[Passenger]:
    passengers = []
    open_seats = {d.key: seat_rng.sample(SEATS, len(SEATS)) for d in day.departures}
    for flight in day.arrivals:
        hero = day.live and flight is day.hero
        if hero:
            plan = [True] * HERO_CONNECTING + [False] * HERO_LOCAL
            rng.shuffle(plan)
            low, high = HERO_BUFFER
        else:
            plan = [rng.random() < CONNECTING_SHARE for _ in range(rng.randint(*PASSENGERS_PER_FLIGHT))]
            low, high = CONNECTION_BUFFER
        candidates = [d for d in day.departures if d.destination != flight.origin
                      and timedelta(minutes=low) <= d.scheduled - flight.scheduled <= timedelta(minutes=high)]
        seats = seat_rng.sample(SEATS, len(plan))
        for seq, (connects, seat) in enumerate(zip(plan, seats, strict=True), start=1):
            passenger = Passenger(f"P-{day.service_date:%m%d}-{flight.number:03d}-{seq:03d}", flight, seat)
            if connects and candidates:
                for onward in rng.sample(candidates, min(3, len(candidates))):
                    valid, flip = _risk_flip(flight, onward)
                    if valid:
                        passenger.connecting, passenger.impacted_at = onward, flip
                        passenger.connecting_seat = open_seats[onward.key].pop()
                        break
            passengers.append(passenger)
    return passengers


def _band(delay: int) -> tuple[bool, bool]:
    """What a delay shows on the dashboard: DELAYED from 15 minutes, an at-risk map color from 45."""
    return delay >= DELAYED_AT, delay >= MAP_AT_RISK


def _keeps_risk(inbound: Flight, onward, at: datetime, until: datetime, base: int, delay: int) -> bool:
    """Whether estimating `delay` rather than `base` over [at, until) leaves every connection's risk as is."""
    limit = timedelta(minutes=CONNECTION_MINUTES)
    for flight in onward:
        for when in [at, *(w for w, _ in flight.reveals if at < w < until)]:
            gap = flight.estimated_at(when) - inbound.scheduled
            if (gap - timedelta(minutes=delay) < limit) != (gap - timedelta(minutes=base) < limit):
                return False
    return True


def _revise_estimates(day: Day, rng: random.Random) -> None:
    """Live-stream arrival-time revisions that never change a status, map color, or connection's risk."""
    start = day.hero.scheduled - HERO_LEAD
    end = start + timedelta(minutes=STREAM_MINUTES)
    onward: dict[str, dict[str, Flight]] = {}
    for passenger in day.passengers:
        if passenger.connecting:
            onward.setdefault(passenger.inbound.key, {})[passenger.connecting.key] = passenger.connecting
    for flight in day.arrivals:
        updates = list(flight.ticks(start, flight.estimated_at() - SETTLE_BEFORE))
        if flight is day.hero or len(updates) < 2:
            continue
        settle = (updates[-1], flight.delay_at(updates[-1]))
        announced = {w for w, _ in flight.reveals}
        revisions: list[tuple[datetime, int]] = []
        for at in updates[:-1]:
            if at > end:
                break
            if at in announced or rng.random() >= REVISION_CHANCE:
                continue
            base = flight.delay_at(at)
            delay = base + rng.choice(REVISION_MINUTES)
            until = min([w for w in announced if w > at] + [settle[0]])
            values = [d for _, d in sorted([*flight.reveals, *revisions, (at, delay), settle])]
            if (_band(delay) == _band(base) and max(values) - values[-1] <= 3
                    and all(later >= earlier - 3 for earlier, later in pairwise(values))
                    and _keeps_risk(flight, onward.get(flight.key, {}).values(), at, until, base, delay)):
                revisions.append((at, delay))
        if revisions:
            flight.reveals = sorted([*flight.reveals, *revisions, settle])


def build_day(seed: int, service_date: date, midnight: datetime, live: bool = False,
              schedule_only: bool = False) -> Day:
    """One service day. The same seed, date, and variant always produce the same keys and values."""
    variant = "live" if live else "schedule" if schedule_only else "history"
    rng = random.Random(f"{seed}:{service_date.isoformat()}:{variant}")
    suffix = f"{service_date:%Y%m%d}"
    numbers = iter(range(101, 101 + len(ARRIVAL_MINUTES)))
    arrivals = []
    for i, minute in enumerate(ARRIVAL_MINUTES):
        hero = minute == HERO_MINUTE
        number = HERO_NUMBER if hero else next(numbers)
        arrivals.append(Flight(f"{AIRLINE}{number}-{suffix}", number, HERO_ORIGIN if hero else SPOKES[i % 15],
                               HUB, midnight + timedelta(minutes=minute), 0 if hero else rng.randrange(60)))
    departures = [Flight(f"{AIRLINE}{601 + i}-{suffix}", 601 + i, HUB, SPOKES[i % 15],
                         midnight + timedelta(minutes=minute), rng.randrange(60))
                  for i, minute in enumerate(DEPARTURE_MINUTES)]
    day = Day(service_date, live, arrivals, departures, [])
    if not schedule_only:
        _assign_delays(day, rng)
        # Seats draw from their own stream, so they never shift delays, connections, or offers.
        day.passengers = _passengers(day, rng, random.Random(f"{seed}:{service_date.isoformat()}:{variant}:seats"))
    return day


def _recoveries(day: Day, next_day: Day, seed: int, sellout_at: datetime | None) -> list[Recovery]:
    rng = random.Random(f"{seed}:{day.service_date.isoformat()}:{'live' if day.live else 'history'}:recovery")
    by_destination: dict[str, list[Flight]] = {}
    for flight in day.departures + next_day.departures:
        by_destination.setdefault(flight.destination, []).append(flight)

    def hotel_for(index: int, at: datetime) -> str:
        """The index-th hotel in HOTEL_ORDER that still has rooms: the Grand Hyatt is gone from the sellout on."""
        open_hotels = [h for h in HOTEL_ORDER if h != GRAND_HYATT or sellout_at is None or at < sellout_at]
        return open_hotels[index]

    recoveries = []
    for passenger in day.passengers:
        if passenger.impacted_at is None:
            continue
        landed = passenger.inbound.estimated_at()
        ready = landed + timedelta(minutes=CONNECTION_MINUTES)
        options = [f for f in by_destination[passenger.connecting.destination] if f.scheduled >= ready][:2]
        if not options:
            continue
        recommended_at = passenger.impacted_at + timedelta(seconds=rng.randint(*AGENT_LATENCY_SECONDS))
        hotels = []
        for index, flight in enumerate(options):
            if flight.scheduled - landed < OVERNIGHT:
                hotels.append(None)
            else:
                hotels.append(hotel_for(index, recommended_at))
        books = rng.random() < BOOKING_SHARE
        decided_at = recommended_at + timedelta(seconds=rng.randint(*DECISION_SECONDS))
        choice = 0 if rng.random() < 0.7 or len(options) == 1 else 1
        if books and hotels[choice] == GRAND_HYATT and sellout_at and decided_at >= sellout_at and len(options) > 1:
            choice = 1 - choice
        recoveries.append(Recovery(
            passenger, options, hotels, recommended_at,
            resolved_at=decided_at if books else recommended_at + OFFER_EXPIRY,
            booked=choice if books else None,
            completes=not (day.live and passenger.inbound is day.hero)))
    return recoveries


def build_plan(start: datetime, seed: int = SEED, history: bool = True) -> Plan:
    """History days, the live day starting at `start`, and tomorrow's schedule for overnight rebookings."""
    midnight = start + HERO_LEAD - timedelta(minutes=HERO_MINUTE)
    today = start.date()
    days = [build_day(seed, today - timedelta(days=back), midnight - timedelta(days=back))
            for back in range(HISTORY_DAYS if history else 0, 0, -1)]
    days.append(build_day(seed, today, midnight, live=True))
    _revise_estimates(days[-1], random.Random(f"{seed}:{today.isoformat()}:live:revisions"))
    tomorrow = build_day(seed, today + timedelta(days=1), midnight + timedelta(days=1), schedule_only=True)
    recoveries = []
    for day, next_day in zip(days, [*days[1:], tomorrow], strict=True):
        recoveries += _recoveries(day, next_day, seed, start + SELLOUT if day.live else None)
    return Plan(start, days, tomorrow, recoveries, seed)


def passenger_to_follow(plan: Plan) -> Passenger:
    """The first passenger on the live day's RA417 who misses a connection; the stream names them at the start."""
    hero = plan.live.hero
    return next(p for p in plan.live.passengers if p.inbound is hero and p.impacted_at)


def _hotel(name: str, rooms: int) -> dict:
    return dict(available_rooms=rooms, nightly_rate=HOTEL_RATES[name])


def _hotel_rooms(plan: Plan, until: datetime):
    """(time, hotel, rooms) every HOTEL_EVERY as guests book and cancel; the Grand Hyatt sells out at SELLOUT."""
    rng = random.Random(f"{plan.seed}:{plan.start.isoformat()}:hotels")
    hyatt, marriott, hilton = GRAND_HYATT_ROOMS, MARRIOTT_ROOMS, HILTON_ROOMS
    for step in range(1, (until - plan.start) // HOTEL_EVERY + 1):
        left = SELLOUT // HOTEL_EVERY - step  # updates until the Grand Hyatt sells out
        if left > 0:
            hyatt = max(1, min(GRAND_HYATT_ROOMS, left, hyatt + rng.choices((-1, 0, 1), (45, 35, 20))[0]))
        else:
            hyatt = 0
        marriott = max(1, marriott + rng.choices((-1, 0, 1), (25, 55, 20))[0])
        hilton = max(1, hilton + rng.choices((-1, 0, 1), (25, 55, 20))[0])
        at = plan.start + HOTEL_EVERY * step
        yield at, GRAND_HYATT, hyatt
        yield at, MARRIOTT, marriott
        yield at, HILTON, hilton


def hotel_warmup(plan: Plan, updates: int = 200):
    """At least `updates` hotel rows from the morning before the stream, ending at the starting rooms.

    The reset sends them through the hotel webhook so its connector has traffic before Demo 2 starts.
    """
    rng = random.Random(f"{plan.seed}:{plan.start.isoformat()}:warmup")
    rooms = {GRAND_HYATT: GRAND_HYATT_ROOMS, MARRIOTT: MARRIOTT_ROOMS, HILTON: HILTON_ROOMS}
    steps = []
    for _ in range(-(-updates // len(rooms))):
        steps.append(dict(rooms))
        # Walking back in time: rooms were freer earlier, since guests book through the morning.
        rooms = {name: min(full, max(1, rooms[name] + rng.choices((-1, 0, 1), (20, 50, 30))[0]))
                 for name, full in ((GRAND_HYATT, GRAND_HYATT_ROOMS), (MARRIOTT, MARRIOTT_ROOMS * 2),
                                    (HILTON, HILTON_ROOMS * 2))}
    for step in reversed(steps):
        for name in HOTEL_ORDER:
            yield HOTELS, name, _hotel(name, step[name])


def initial_records(plan: Plan):
    """Every row as of the stream start: history final state, the live day so far, tomorrow's schedule."""
    at = plan.start
    for name, rooms in ((GRAND_HYATT, GRAND_HYATT_ROOMS), (MARRIOTT, MARRIOTT_ROOMS), (HILTON, HILTON_ROOMS)):
        yield HOTELS, name, _hotel(name, rooms)
    for day in [*plan.days, plan.tomorrow]:
        for flight in day.flights:
            yield FLIGHTS, flight.key, flight.row(at)
    for day in plan.days:
        for passenger in day.passengers:
            # History keeps only connecting passengers; nobody else can be impacted.
            if day.live or passenger.connecting:
                yield ITINERARIES, passenger.key, dict(
                    inbound_flight_id=passenger.inbound.key,
                    inbound_seat=passenger.inbound_seat,
                    connecting_flight_id=passenger.connecting.key if passenger.connecting else None,
                    connecting_seat=passenger.connecting_seat)
    for recovery in plan.recoveries:
        for key, value in recovery.rows(at):
            yield OFFERS, key, value


def stream_events(plan: Plan, minutes: int) -> list[tuple[datetime, str, str, dict]]:
    """Timed live updates after the stream start: flights, hotel rooms, and offers."""
    start, end = plan.start, plan.start + timedelta(minutes=minutes)
    events = []
    for flight in plan.live.flights:
        events += [(tick, FLIGHTS, flight.key, flight.row(tick)) for tick in flight.ticks(start, end)]
    events += [(at, HOTELS, hotel, _hotel(hotel, rooms)) for at, hotel, rooms in _hotel_rooms(plan, end)]
    for recovery in plan.recoveries:
        if recovery.passenger.inbound not in plan.live.arrivals:
            continue
        moments = [recovery.recommended_at] + ([recovery.resolved_at] if recovery.completes else [])
        for at in moments:
            if start < at <= end:
                events += [(at, OFFERS, key, value) for key, value in recovery.rows(at)]
    events.sort(key=lambda event: event[0])
    return events


def reset_keys(start: datetime, seed: int = SEED) -> dict[str, set[str]]:
    """Every key a run near `start` could have written, with a few days of margin either side."""
    keys = {FLIGHTS: set(), ITINERARIES: set(), HOTELS: set(HOTEL_RATES), OFFERS: set()}
    for back in range(-3, HISTORY_DAYS + 4):
        service_date = start.date() - timedelta(days=back)
        for live in (True, False):
            day = build_day(seed, service_date, start, live=live)
            keys[FLIGHTS].update(f.key for f in day.flights)
            for passenger in day.passengers:
                keys[ITINERARIES].add(passenger.key)
                if passenger.impacted_at:
                    keys[OFFERS].update({f"{passenger.key}-O1", f"{passenger.key}-O2"})
    return keys


class Publisher:
    def __init__(self, credentials: dict | None, dry_run: bool):
        self.dry_run = dry_run
        self.serializers = {}
        self.string = StringSerializer("utf_8")
        if dry_run:
            return
        credentials = credentials or {}
        # With a webhook, hotel updates arrive the way a hotel partner's would: over HTTPS.
        self.webhook = credentials.get("hotel_webhook_url")
        self.webhook_auth = (credentials.get("hotel_webhook_api_key", ""),
                             credentials.get("hotel_webhook_api_secret", ""))
        self.schema_registry = SchemaRegistryClient({
            "url": credentials["schema_registry_url"],
            "basic.auth.user.info": (
                f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}")})
        self.producer = Producer({
            "bootstrap.servers": credentials["bootstrap_servers"],
            "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
            "sasl.username": credentials["kafka_api_key"],
            "sasl.password": credentials["kafka_api_secret"],
            "linger.ms": 20,
        })

    def publish(self, topic: str, key: str, value: dict | None) -> None:
        if self.dry_run:
            print(json.dumps({"topic": topic, "key": key, "value": value}, default=str))
            return
        if topic == HOTELS and value is not None and self.webhook:
            self._post_hotel(key, value)
            return
        if topic not in self.serializers:
            registered = self.schema_registry.get_latest_version(f"{topic}-value")
            schema = json.loads(registered.schema.schema_str)
            self.serializers[topic] = (
                AvroSerializer(self.schema_registry, registered.schema.schema_str,
                               conf={"auto.register.schemas": False, "use.latest.version": True}),
                {field["name"]: field["type"] for field in schema["fields"]},
            )
        serializer, fields = self.serializers[topic]
        if value is not None:
            value = dict(value)
            for name, field_type in fields.items():
                if name not in value or not isinstance(value[name], datetime):
                    continue
                variants = field_type if isinstance(field_type, list) else [field_type]
                if any(isinstance(t, dict) and t.get("logicalType") == "timestamp-millis" for t in variants):
                    value[name] = value[name].replace(tzinfo=timezone.utc)
        context = SerializationContext(topic, MessageField.VALUE)
        payload = serializer(value, context) if value is not None else None
        encoded_key = self.string(key, SerializationContext(topic, MessageField.KEY))
        while True:
            try:
                self.producer.produce(topic, key=encoded_key, value=payload, partition=0)
                break
            except BufferError:
                self.producer.poll(1)
        self.producer.poll(0)

    def _post_hotel(self, name: str, value: dict) -> None:
        """Send one hotel update to the webhook and wait for its acknowledgement, so updates keep their order."""
        body = dict(hotel_name=name, available_rooms=value["available_rooms"], nightly_rate=str(value["nightly_rate"]))
        for attempt in range(WEBHOOK_ATTEMPTS):
            try:
                response = requests.post(self.webhook, json=body, auth=self.webhook_auth, timeout=30)
                if response.ok:
                    return
                problem = f"{response.status_code} {response.text[:200]}"
            except requests.RequestException as error:
                problem = str(error)
            if attempt == WEBHOOK_ATTEMPTS - 1:
                raise RuntimeError(f"The hotel webhook rejected {name}: {problem}")
            time.sleep(2 ** attempt)  # a retry can duplicate a record; hotel_inventory keeps only the latest

    def flush(self) -> None:
        if not self.dry_run:
            pending = self.producer.flush(60)
            if pending:
                raise RuntimeError(f"{pending} records were not delivered")


def _pid_file():
    return get_project_root() / "tmp" / "datagen.pid"


def _start_file():
    """The last stream's start, which --resume picks back up."""
    return get_project_root() / "tmp" / "datagen.start"


def stop_previous_stream() -> None:
    """Stop a stream left running by an earlier deploy or run, so two streams never interleave."""
    path = _pid_file()
    try:
        pid = int(path.read_text().strip())
    except (OSError, ValueError):
        return
    if pid != os.getpid():
        try:
            command = subprocess.run(["ps", "-p", str(pid), "-o", "command="],
                                     capture_output=True, text=True, check=False).stdout
        except OSError:
            command = ""
        if "airport-datagen" in command or "airport_datagen" in command:
            os.kill(pid, signal.SIGTERM)
            logging.info("Stopped the previous data stream (pid %d)", pid)
    path.unlink(missing_ok=True)


def agent_enabled() -> bool:
    """True when terraform/airline-demo runs the recovery agent (its recovery_agent_enabled output)."""
    state = get_project_root() / "terraform" / "airline-demo" / "terraform.tfstate"
    try:
        return bool(json.loads(state.read_text())["outputs"]["recovery_agent_enabled"]["value"])
    except (OSError, KeyError, ValueError):
        return False


def _due(start: datetime, at: datetime, speed: float) -> datetime:
    """When to publish an event stamped `at`: stream time passes `speed` times faster than the clock."""
    return start + (at - start) / speed


def _catch_up(events: list, start: datetime, now: datetime, speed: float) -> list:
    """The events, with the ones already due collapsed to each key's latest.

    A resumed stream then catches up at once instead of replaying, say, RA417's whole delay ramp,
    which would turn its passengers back to OK for a moment.
    """
    due = 0
    while due < len(events) and _due(start, events[due][0], speed) <= now:
        due += 1
    latest = {(topic, key): (at, topic, key, value) for at, topic, key, value in events[:due]}
    return sorted(latest.values(), key=lambda event: event[0]) + events[due:]


def run(start: datetime, seed: int = SEED, history: bool = True, minutes: int = STREAM_MINUTES,
        dry_run: bool = False, stream_only: bool = False, agent_offers: bool = False,
        speed: float = 1.0) -> None:
    plan = build_plan(start, seed, history=history and not stream_only)
    if agent_offers:
        plan.recoveries = [r for r in plan.recoveries if r.passenger.inbound is not plan.live.hero]
        logging.info("The recovery agent writes %s's offers; the generator writes the rest", plan.live.hero.key)
    publisher = Publisher(None if dry_run else extract_kafka_credentials("aws", get_project_root()), dry_run)
    if not dry_run:
        stop_previous_stream()
    if not stream_only:
        count = 0
        for topic, key, value in initial_records(plan):
            publisher.publish(topic, key, value)
            count += 1
        publisher.flush()
        logging.info("Published %d records: %d history days, today, and tomorrow's schedule",
                     count, len(plan.days) - 1)
    events = stream_events(plan, minutes)
    if not events:
        return
    pid_file = _pid_file()
    if not dry_run:
        pid_file.parent.mkdir(exist_ok=True)
        pid_file.write_text(str(os.getpid()))
        _start_file().write_text(f"{plan.start.isoformat()} {speed:g}")
        events = _catch_up(events, plan.start, datetime.now(timezone.utc).replace(tzinfo=None), speed)
    hero = plan.live.hero
    logging.info("Streaming %d updates for %d minutes (%.4g minutes of waiting at %gx); %s is scheduled at %s UTC",
                 len(events), minutes, minutes / speed, speed, hero.key, f"{hero.scheduled:%H:%M}")
    follow = passenger_to_follow(plan)
    logging.info("Passenger to follow: %s, seat %s on %s, connecting to %s for %s", follow.key,
                 follow.inbound_seat, hero.key, follow.connecting.key, follow.connecting.destination)
    impacted = [p.impacted_at for p in plan.live.passengers if p.inbound is hero and p.impacted_at]
    end = plan.start + timedelta(minutes=minutes)
    for moment, at in ((f"{hero.key}'s first passenger turns HIGH", min(impacted)),
                       (f"{hero.key}'s last passenger turns HIGH", max(impacted)),
                       (f"{GRAND_HYATT} sells out", plan.start + SELLOUT)):
        if at <= end:
            logging.info("%s at %s UTC", moment, f"{_due(plan.start, at, speed):%H:%M}")
    try:
        minute, sent = None, 0
        for at, topic, key, value in events:
            if not dry_run:
                wait = (_due(plan.start, at, speed) - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()
                if wait > 0:
                    time.sleep(wait)
            publisher.publish(topic, key, value)
            sent += 1
            if minute != at.replace(second=0):
                if minute is not None:
                    logging.info("%s UTC: %d updates", f"{minute:%H:%M}", sent)
                minute, sent = at.replace(second=0), 0
        publisher.flush()
        logging.info("Stream finished")
    finally:
        if not dry_run and pid_file.exists() and pid_file.read_text().strip() == str(os.getpid()):
            pid_file.unlink()


def reset(start: datetime, seed: int, dry_run: bool) -> None:
    publisher = Publisher(None if dry_run else extract_kafka_credentials("aws", get_project_root()), dry_run)
    if not dry_run:
        stop_previous_stream()
    for topic, keys in reset_keys(start, seed).items():
        for key in sorted(keys):
            publisher.publish(topic, key, None)
        logging.info("%s: wrote %d tombstones", topic, len(keys))
    publisher.flush()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="uv run airport-datagen", description="River Air flight recovery data")
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--now", help="ISO-8601 UTC stream start (default: now)")
    parser.add_argument("--minutes", type=int, default=STREAM_MINUTES,
                        help=f"minutes of live updates to stream (default: {STREAM_MINUTES}; 0 exits after publishing)")
    parser.add_argument("--speed", type=float, default=1.0,
                        help="play the stream this many times faster, e.g. 5 runs 90 minutes in 18 (default: 1). "
                             "Event timestamps keep stream time, so the dashboard's clock runs ahead of the wall clock")
    parser.add_argument("--skip-history", action="store_true", help=f"skip the {HISTORY_DAYS} days of history")
    parser.add_argument("--stream-only", action="store_true",
                        help="stream the live updates for an earlier --now without republishing")
    parser.add_argument("--dry-run", action="store_true", help="print records as JSON; no Kafka writes, no waiting")
    parser.add_argument("--reset", action="store_true", help="write tombstones for every generated key")
    parser.add_argument("--offers", choices=("auto", "agent", "generator"), default="auto",
                        help="who writes RA417's offers (default auto: the agent when Terraform runs it)")
    parser.add_argument("--resume", action="store_true",
                        help="pick the last stream back up where its clock is now, at its start and speed, "
                             "without republishing; with --offers generator, RA417's missed offers arrive at once")
    args = parser.parse_args(argv)
    if args.speed <= 0:
        parser.error("--speed must be greater than 0")
    if args.resume:
        if args.now or args.stream_only or args.reset:
            parser.error("--resume sets its own --now and --stream-only, and can't be combined with --reset")
        try:
            args.now, speed = _start_file().read_text().split()
        except (OSError, ValueError):
            parser.error("there's no stream to resume yet; start one with `uv run airport-datagen`")
        args.speed, args.stream_only = float(speed), True
    setup_logging()
    start = _clock(args.now)
    try:
        if args.reset:
            reset(start, args.seed, args.dry_run)
        else:
            agent = args.offers == "agent" or (args.offers == "auto" and agent_enabled())
            run(start, args.seed, history=not args.skip_history, minutes=args.minutes,
                dry_run=args.dry_run, stream_only=args.stream_only, agent_offers=agent, speed=args.speed)
    except KeyboardInterrupt:
        logging.info("Stopped")


if __name__ == "__main__":
    main()
