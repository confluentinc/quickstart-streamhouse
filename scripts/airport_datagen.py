"""River Air flight recovery data: 30 days of history plus one live day at the SFO hub.

Each service day has 180 flights: 90 arrivals into SFO and 90 departures from it. Every
flight uses one 182-seat layout, modeled after a two-class Boeing 737-900ER, and each
passenger has a seat on each of their flights.
The live day is shifted so flight RA417 from Chicago is scheduled 50 minutes after the
stream starts. Its delay grows every 30 seconds from +5 to +25 minutes, and 150 of its 170
passengers miss their connection. RA183 from Boston and RA185 from Las Vegas follow the same
script later in the stream (AGENT_FLIGHTS). Every active flight publishes its status every 30 seconds,
and on the live day each growing delay climbs a little with every update, as RA417's does.
En-route arrivals revise their estimate by a minute or two along the way, and the three
hotels publish their rooms every 15 seconds. Announced delays only grow (a severe delay may
shrink by at most three minutes near landing), so a flight never goes from hours late back
to on time.

The generator writes two rebooking offers a few seconds after a connection becomes HIGH
risk. Past bookings are completed the way customers would complete them; the offers for
those three flights stay open for the presenter. When the recovery agent runs in Flink
(sql/26-32), it writes their offers and the generator writes every other flight's.

    uv run airport-datagen                  # history + live day, then stream 90 minutes
    uv run airport-datagen --offers generator  # write the agent flights' offers even if the agent runs
    uv run airport-datagen --speed 5        # play the 90-minute stream in 18 minutes
    uv run airport-datagen --minutes 0      # publish the current state and exit
    uv run airport-datagen --resume         # pick a stopped stream back up on its original clock
    uv run airport-datagen --dry-run --skip-history --minutes 5
    uv run airport-datagen --reset          # tombstone every generated key

The continuous mode runs around the clock instead: each UTC day has 180 flights, eight RA417s from Chicago
three hours apart, and a Grand Hyatt that sells out at 18:00 and refills at midnight. Every row follows from
the seed and the wall clock, so a restarted stream picks up where it was. The recovery agent writes every offer;
the stream answers them as the customers would, unless someone already decided in the app. At each UTC
midnight it publishes the day after next and tombstones the day RETENTION_DAYS back.

    uv run airport-datagen --continuous                # stream forever from now
    uv run airport-datagen --continuous --history      # first publish the 29 finished days before yesterday
    uv run airport-datagen --continuous --dry-run --now 2026-11-12T14:00:00Z  # print an hour as JSON
"""

import argparse
import heapq
import json
import logging
import os
import random
import signal
import subprocess
import time
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from itertools import count, pairwise
from operator import itemgetter

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
    1330,
    1335,
    1340,
    1345,
    1350,
    1356,
    1362,
    1368,
    1374,
    1380,
    1386,
    1392,
    1398,
    1404,
    1410,
)
LAST_BANK = 75

# The around-the-clock template: 90 arrivals 16 minutes apart. Each destination's 6 departures run
# 3 h 24 m apart with one 7-hour overnight gap, and the gaps start 96 minutes apart.
CONTINUOUS_ARRIVAL_MINUTES = tuple(i * 16 for i in range(90))


def _continuous_departure(i: int) -> int:
    destination, k = i % 15, i // 15
    return ((destination * 96 + 420) % 1440 + k * 204) % 1440


CONTINUOUS_DEPARTURE_MINUTES = tuple(_continuous_departure(i) for i in range(90))


@dataclass(frozen=True)
class Schedule:
    arrivals: tuple[int, ...]
    departures: tuple[int, ...]
    scripted: bool
    variant: str


SCRIPTED = Schedule(ARRIVAL_MINUTES, DEPARTURE_MINUTES, True, "")
AROUND_THE_CLOCK = Schedule(CONTINUOUS_ARRIVAL_MINUTES, CONTINUOUS_DEPARTURE_MINUTES, False, "continuous")

# Every flight's seat map, modeled after a two-class Boeing 737-900ER: First in rows 1-5
# (A C | D F) and Economy in rows 6-32 (A B C | D E F), 182 seats.
SEATS = tuple(f"{row}{letter}" for row in range(1, 33) for letter in ("ACDF" if row <= 5 else "ABCDEF"))

HERO_NUMBER = 417
HERO_ORIGIN = "ORD"
HERO_MINUTE = 21 * 60 + 40
CONTINUOUS_HERO_MINUTES = tuple(40 + 180 * k for k in range(8))  # 00:40, 03:40, ... 21:40
HERO_LEAD = timedelta(minutes=50)
# From +5 to +25 minutes into the stream, each 30-second RA417 update adds to its delay, up to 115.
HERO_RAMP = (timedelta(minutes=5), timedelta(minutes=25))
HERO_DELAY = 115
HERO_CONNECTING = 150
HERO_LOCAL = 20
HERO_BUFFER = (50, 110)
# On an around-the-clock day each RA417's delay climbs from 75 to 25 minutes before its scheduled arrival.
CONTINUOUS_HERO_RAMP = (timedelta(minutes=-75), timedelta(minutes=-25))


# Two later arrivals the recovery agent also handles (sql/27-staging-impacted-passengers.sql), so its
# offers come in three waves. Like RA417's, each delay climbs over a window of stream minutes that
# ends about 25 minutes before the flight is due, where the map starts its descent, and it breaks every
# connection onto the on-time last bank. RA183's passengers turn HIGH before the Grand Hyatt sells
# out, RA185's after.
@dataclass(frozen=True)
class AgentFlight:
    ramp: tuple[timedelta, timedelta]  # minutes into the stream the delay climbs from and to
    delay: int
    connecting: int
    local: int


AGENT_FLIGHTS = {
    183: AgentFlight((timedelta(minutes=21), timedelta(minutes=36)), 75, 40, 120),  # from Boston, due at +61
    185: AgentFlight((timedelta(minutes=49), timedelta(minutes=59)), 60, 30, 130),  # from Las Vegas, due at +84
}

CONNECTION_MINUTES = 45  # Flink's HIGH threshold in sql/24-serving-passenger-state.sql
CONNECTING_SHARE = 0.3
CONNECTION_BUFFER = (60, 180)
PASSENGERS_PER_FLIGHT = (120, 180)
SEVERE_PER_DAY = 10
CONTINUOUS_SEVERE_PER_DAY = 14  # an around-the-clock day has no forced last bank, so it needs more to strand as many
SEVERE_DELAY = (90, 240)
MODERATE_SHARE = 0.25
MODERATE_DELAY = (15, 60)
DELAYED_AT = 15
MAP_AT_RISK = 45  # the dashboard's flight map colors a flight this many minutes late as at risk
ACTIVE_BEFORE = timedelta(minutes=180)
UPDATE_EVERY = timedelta(seconds=30)  # how often an active flight publishes its status
BOARDING = timedelta(minutes=30)
# Live-stream arrival-time revisions: chance per update (about one a minute), minutes off the
# announced delay, and when they stop.
REVISION_CHANCE = 0.4
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
RETENTION_DAYS = 35  # the continuous stream tombstones a service day this many days after it
STREAM_MINUTES = 90
SEED = 42


@dataclass
class Flight:
    key: str
    number: int
    origin: str
    destination: str
    scheduled: datetime
    offset: int = 0  # shifts this flight's updates by this many seconds, so flights don't all publish at once
    reveals: list[tuple[datetime, int]] = field(default_factory=list)  # (published at, delay minutes), in time order

    @property
    def arrival(self) -> bool:
        return self.destination == HUB

    def delay_at(self, at: datetime) -> int:
        announced = bisect_right(self.reveals, at, key=itemgetter(0))
        return self.reveals[announced - 1][1] if announced else 0

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
        return dict(
            origin=self.origin,
            destination=self.destination,
            scheduled_time=self.scheduled,
            estimated_time=self.estimated_at(at),
            status=self.status_at(at),
        )

    def ticks(self, after: datetime, until: datetime):
        """Update times in (after, until], one per UPDATE_EVERY, from three hours out to completion."""
        tick = self.scheduled - ACTIVE_BEFORE + timedelta(seconds=self.offset)
        last = self.estimated_at() + timedelta(seconds=self.offset)
        if tick <= after:
            tick += UPDATE_EVERY * ((after - tick) // UPDATE_EVERY + 1)
        while tick <= min(last, until):
            yield tick
            tick += UPDATE_EVERY


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
            yield (
                f"{self.passenger.key}-O{index + 1}",
                dict(
                    passenger_id=self.passenger.key,
                    recommended_flight_id=flight.key,
                    hotel_name=hotel,
                    status=status,
                    hotel_cost=HOTEL_RATES[hotel] if hotel else None,
                    impacted_at=self.passenger.impacted_at,
                    recommended_at=self.recommended_at,
                ),
            )


@dataclass
class Day:
    service_date: date
    live: bool
    arrivals: list[Flight]
    departures: list[Flight]
    passengers: list[Passenger]
    schedule: Schedule = SCRIPTED

    @property
    def flights(self) -> list[Flight]:
        return self.arrivals + self.departures

    @property
    def hero(self) -> Flight:
        if not self.schedule.scripted:
            raise ValueError("an around-the-clock day has several RA417s; use heroes")
        return self.arrivals[ARRIVAL_MINUTES.index(HERO_MINUTE)]

    @property
    def heroes(self) -> list[Flight]:
        """The RA417s: the one scripted flight, or the eight of an around-the-clock day."""
        if self.schedule.scripted:
            return [self.hero]
        return [f for f in self.arrivals if f.number == HERO_NUMBER]

    @property
    def agent_flights(self) -> list[Flight]:
        """The live day's flights the recovery agent writes offers for: RA417, then AGENT_FLIGHTS."""
        if not self.schedule.scripted:
            return []
        return [self.hero, *(f for f in self.arrivals if f.number in AGENT_FLIGHTS)]


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


def _utc(value: str) -> datetime:
    """An ISO-8601 time as naive UTC; one without a zone is taken as UTC."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _clock(value: str | None) -> datetime:
    """Stream start as naive UTC, trimmed to the minute."""
    dt = _utc(value) if value else datetime.now(timezone.utc).replace(tzinfo=None)
    return dt.replace(second=0, microsecond=0)


def _climb(first: datetime, last: datetime, delay: int) -> list[tuple[datetime, int]]:
    """A delay that grows with every update after `first`, reaching `delay` at `last`, the way RA417's does."""
    steps = (last - first) // UPDATE_EVERY
    return [(first + UPDATE_EVERY * k, round(delay * k / steps)) for k in range(1, steps + 1)]


def _delays(flight: Flight, steps, climb: bool = False) -> None:
    """Announce delays at (minutes from schedule, delay) steps, on this flight's update ticks.

    With `climb`, the delay grows a little with every update from the first step to the
    longest one instead of jumping at each step; a make-up step after that still lands at once.
    """
    notices = [(flight.scheduled + timedelta(minutes=rel, seconds=flight.offset), delay) for rel, delay in steps]
    if climb:
        peak = max(notices, key=itemgetter(1))
        notices = _climb(notices[0][0], *peak) + notices[notices.index(peak) + 1 :]
    flight.reveals = notices


def _assign_delays(day: Day, rng: random.Random) -> None:
    story = day.live and day.schedule.scripted
    hero = day.hero if story else None
    scripted = {f.key: AGENT_FLIGHTS[f.number] for f in day.agent_flights[1:]} if story else {}
    forced = {f.key for f in day.departures[LAST_BANK:]} if story else set()
    # An around-the-clock live day climbs each of its eight RA417s the same way, relative to its own schedule.
    ramps = {id(f) for f in day.heroes} if day.live and not day.schedule.scripted else set()
    pool = [
        f for f in day.flights if f is not hero and id(f) not in ramps and f.key not in forced and f.key not in scripted
    ]
    severe_count = CONTINUOUS_SEVERE_PER_DAY if ramps else SEVERE_PER_DAY - (1 if hero else 0)
    severe = {f.key for f in rng.sample(pool, severe_count)}
    # The live day streams its updates, so its delays climb the way RA417's does. History days
    # publish only their final rows, so they keep their two or three notices and the
    # historical answers stay the same.
    climb = day.live
    for flight in day.flights:
        if flight is hero:
            start, (first, last) = flight.scheduled - HERO_LEAD, HERO_RAMP
            flight.reveals = _climb(start + first, start + last, HERO_DELAY)
        elif id(flight) in ramps:
            first, last = CONTINUOUS_HERO_RAMP
            flight.reveals = _climb(flight.scheduled + first, flight.scheduled + last, HERO_DELAY)
        elif flight.key in scripted:
            # On the minute, like RA417, so its passengers turn HIGH at the same stream minutes every day.
            script, flight.offset, start = scripted[flight.key], 0, hero.scheduled - HERO_LEAD
            flight.reveals = _climb(start + script.ramp[0], start + script.ramp[1], script.delay)
        elif flight.key in forced:
            continue
        elif flight.key in severe:
            delay = rng.randint(*SEVERE_DELAY)
            steps = [(-120, round(delay * 0.3)), (-80, round(delay * 0.65)), (-40, delay)]
            makeup = rng.randint(0, 3) if flight.arrival else 0
            if makeup:
                steps.append((delay - 20, delay - makeup))
            _delays(flight, steps, climb)
        elif rng.random() < MODERATE_SHARE:
            delay = rng.randint(*MODERATE_DELAY)
            _delays(flight, [(-90, delay // 2), (-45, delay)], climb)
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
    story = day.live and day.schedule.scripted
    ramps = {id(f) for f in day.heroes} if day.live and not day.schedule.scripted else set()
    for flight in day.arrivals:
        hero = story and flight is day.hero
        script = AGENT_FLIGHTS.get(flight.number) if story else None
        if hero or id(flight) in ramps:
            plan = [True] * HERO_CONNECTING + [False] * HERO_LOCAL
            rng.shuffle(plan)
            low, high = HERO_BUFFER
        elif script:
            plan = [True] * script.connecting + [False] * script.local
            rng.shuffle(plan)
            low, high = CONNECTION_BUFFER
        else:
            plan = [rng.random() < CONNECTING_SHARE for _ in range(rng.randint(*PASSENGERS_PER_FLIGHT))]
            low, high = CONNECTION_BUFFER
        candidates = [
            d
            for d in day.departures
            if d.destination != flight.origin
            and timedelta(minutes=low) <= d.scheduled - flight.scheduled <= timedelta(minutes=high)
        ]
        seats = seat_rng.sample(SEATS, len(plan))
        for seq, (connects, seat) in enumerate(zip(plan, seats, strict=True), start=1):
            if flight.number == HERO_NUMBER and not day.schedule.scripted:
                key = f"P-{day.service_date:%m%d}-417-{flight.scheduled:%H%M}-{seq:03d}"
            else:
                key = f"P-{day.service_date:%m%d}-{flight.number:03d}-{seq:03d}"
            passenger = Passenger(key, flight, seat)
            if connects and candidates:
                # An around-the-clock RA417 tries every onward flight, so each of its connections turns HIGH.
                tries = len(candidates) if id(flight) in ramps else min(3, len(candidates))
                for onward in rng.sample(candidates, tries):
                    valid, flip = _risk_flip(flight, onward)
                    if valid and open_seats[onward.key]:
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


def _revise_estimates(day: Day, rng: random.Random, start: datetime, end: datetime) -> None:
    """Live-stream arrival-time revisions that never change a status, map color, or connection's risk."""
    onward: dict[str, dict[str, Flight]] = {}
    for passenger in day.passengers:
        if passenger.connecting:
            onward.setdefault(passenger.inbound.key, {})[passenger.connecting.key] = passenger.connecting
    scripted = {f.key for f in day.agent_flights + day.heroes}
    for flight in day.arrivals:
        updates = list(flight.ticks(start, flight.estimated_at() - SETTLE_BEFORE))
        if flight.key in scripted or len(updates) < 2:
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
            if (
                _band(delay) == _band(base)
                and max(values) - values[-1] <= 3
                and all(later >= earlier - 3 for earlier, later in pairwise(values))
                and _keeps_risk(flight, onward.get(flight.key, {}).values(), at, until, base, delay)
            ):
                revisions.append((at, delay))
        if revisions:
            flight.reveals = sorted([*flight.reveals, *revisions, settle])


def build_day(
    seed: int,
    service_date: date,
    midnight: datetime,
    live: bool = False,
    schedule_only: bool = False,
    schedule: Schedule = SCRIPTED,
) -> Day:
    """One service day. The same seed, date, and variant always produce the same keys and values."""
    variant = "live" if live else "schedule" if schedule_only else "history"
    rng = random.Random(f"{seed}:{service_date.isoformat()}:{schedule.variant or variant}")
    suffix = f"{service_date:%Y%m%d}"
    numbers = (n for n in count(101) if n != HERO_NUMBER)
    if schedule.scripted:
        hero_slots = {i: HERO_MINUTE for i, minute in enumerate(schedule.arrivals) if minute == HERO_MINUTE}
    else:  # the arrival slot nearest each RA417 time becomes that RA417, scheduled exactly on it
        hero_slots = {}
        for target in CONTINUOUS_HERO_MINUTES:
            free = (i for i in range(len(schedule.arrivals)) if i not in hero_slots)
            hero_slots[min(free, key=lambda i: abs(schedule.arrivals[i] - target))] = target
    arrivals = []
    for i, slot in enumerate(schedule.arrivals):
        hero = i in hero_slots
        minute = hero_slots[i] if hero else slot
        number = HERO_NUMBER if hero else next(numbers)
        key = f"{AIRLINE}{number}-{suffix}"
        if hero and not schedule.scripted:
            key += f"-{minute // 60:02d}{minute % 60:02d}"
        arrivals.append(
            Flight(
                key,
                number,
                HERO_ORIGIN if hero else SPOKES[i % 15],
                HUB,
                midnight + timedelta(minutes=minute),
                0 if hero else rng.randrange(60),
            )
        )
    departures = [
        Flight(
            f"{AIRLINE}{601 + i}-{suffix}",
            601 + i,
            HUB,
            SPOKES[i % 15],
            midnight + timedelta(minutes=minute),
            rng.randrange(60),
        )
        for i, minute in enumerate(schedule.departures)
    ]
    day = Day(service_date, live, arrivals, departures, [], schedule)
    if not schedule_only:
        _assign_delays(day, rng)
        # Seats draw from their own stream, so they never shift delays, connections, or offers.
        day.passengers = _passengers(day, rng, random.Random(f"{seed}:{service_date.isoformat()}:{variant}:seats"))
    return day


@lru_cache(maxsize=8)
def continuous_day(seed: int, service_date: date) -> Day:
    """One around-the-clock service day (UTC), streamed live the way today's scripted day is."""
    midnight = datetime.combine(service_date, datetime.min.time())
    day = build_day(seed, service_date, midnight, live=True, schedule=AROUND_THE_CLOCK)
    _revise_estimates(
        day,
        random.Random(f"{seed}:{service_date.isoformat()}:continuous:revisions"),
        midnight - ACTIVE_BEFORE,
        midnight + timedelta(days=1),
    )
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

    agent = {f.key for f in day.agent_flights} if day.live else set()  # their offers stay open for the presenter
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
        recoveries.append(
            Recovery(
                passenger,
                options,
                hotels,
                recommended_at,
                resolved_at=decided_at if books else recommended_at + OFFER_EXPIRY,
                booked=choice if books else None,
                completes=passenger.inbound.key not in agent,
            )
        )
    return recoveries


def build_plan(start: datetime, seed: int = SEED, history: bool = True) -> Plan:
    """History days, the live day starting at `start`, and tomorrow's schedule for overnight rebookings."""
    midnight = start + HERO_LEAD - timedelta(minutes=HERO_MINUTE)
    today = start.date()
    days = [
        build_day(seed, today - timedelta(days=back), midnight - timedelta(days=back))
        for back in range(HISTORY_DAYS if history else 0, 0, -1)
    ]
    days.append(build_day(seed, today, midnight, live=True))
    window = days[-1].hero.scheduled - HERO_LEAD
    _revise_estimates(
        days[-1],
        random.Random(f"{seed}:{today.isoformat()}:live:revisions"),
        window,
        window + timedelta(minutes=STREAM_MINUTES),
    )
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


CONTINUOUS_SELLOUT_HOUR = 18  # the Grand Hyatt is sold out from 18:00 UTC until it refills at midnight
HYATT_BAND = 5  # through the day the Grand Hyatt's rooms stay within this many of a steady drain to the sellout


@lru_cache(maxsize=8)
def continuous_hotel_rooms(seed: int, service_date: date) -> list[tuple[datetime, str, int]]:
    """(time, hotel, rooms) for all three hotels every HOTEL_EVERY from midnight to the next midnight.

    Each day starts from the full counts. The Grand Hyatt drains to 0 by CONTINUOUS_SELLOUT_HOUR and stays there:
    its walk stays within HYATT_BAND rooms under a line from full at midnight to 0 at the sellout, so the rooms go
    down visibly all day rather than hitting 1 within the first hour."""
    rng = random.Random(f"{seed}:{service_date.isoformat()}:continuous:hotels")
    midnight = datetime.combine(service_date, datetime.min.time())
    sellout_step = timedelta(hours=CONTINUOUS_SELLOUT_HOUR) // HOTEL_EVERY
    hyatt, marriott, hilton = GRAND_HYATT_ROOMS, MARRIOTT_ROOMS, HILTON_ROOMS
    rows = []
    for step in range(1, timedelta(days=1) // HOTEL_EVERY + 1):
        left = sellout_step - step
        if left > 0:
            ceiling = -(-GRAND_HYATT_ROOMS * left // sellout_step)  # ceil: at least 1 until the sellout
            walk = hyatt + rng.choices((-1, 0, 1), (45, 35, 20))[0]
            hyatt = max(1, ceiling - HYATT_BAND, min(ceiling, walk))
        else:
            hyatt = 0
        marriott = max(1, marriott + rng.choices((-1, 0, 1), (25, 55, 20))[0])
        hilton = max(1, hilton + rng.choices((-1, 0, 1), (25, 55, 20))[0])
        at = midnight + HOTEL_EVERY * step
        rows += [(at, GRAND_HYATT, hyatt), (at, MARRIOTT, marriott), (at, HILTON, hilton)]
    return rows


@lru_cache(maxsize=8)
def _continuous_room_times(seed: int, service_date: date) -> list[datetime]:
    return [at for at, _, _ in continuous_hotel_rooms(seed, service_date)]


def rooms_at(seed: int, at: datetime) -> dict[str, int]:
    """Rooms per hotel as of `at` (UTC): the day's latest row at or before it, or the full counts at midnight."""
    rooms = {GRAND_HYATT: GRAND_HYATT_ROOMS, MARRIOTT: MARRIOTT_ROOMS, HILTON: HILTON_ROOMS}
    service_date = at.date()
    rows = continuous_hotel_rooms(seed, service_date)
    last = bisect_right(_continuous_room_times(seed, service_date), at)  # rows come three to a timestamp
    for _, hotel, count_ in rows[max(0, last - 3) : last]:
        rooms[hotel] = count_
    return rooms


def _midnight(service_date: date) -> datetime:
    return datetime.combine(service_date, datetime.min.time())


def continuous_state(seed: int, now: datetime) -> list[tuple[str, str, dict]]:
    """Every live row as of `now` (UTC): the hotels, then each flight and passenger of yesterday, today, and tomorrow.

    It depends only on the seed and `now`, so a restarted stream republishes what it last streamed, and a later
    `now` never moves a flight back."""
    days = [continuous_day(seed, now.date() + timedelta(days=n)) for n in (-1, 0, 1)]
    rooms = rooms_at(seed, now)
    rows = [(HOTELS, name, _hotel(name, rooms[name])) for name in HOTEL_ORDER]
    rows += [(FLIGHTS, flight.key, flight.row(now)) for day in days for flight in day.flights]
    rows += [(ITINERARIES, p.key, _itinerary(p)) for day in days for p in day.passengers]
    return rows


def _expired_keys(seed: int, service_date: date) -> list[tuple[str, str]]:
    """Every flight, itinerary, and offer key a continuous day wrote, for its tombstones."""
    day = continuous_day.__wrapped__(seed, service_date)  # uncached: an old day is needed once
    keys = [(FLIGHTS, f.key) for f in day.flights] + [(ITINERARIES, p.key) for p in day.passengers]
    for passenger in day.passengers:
        if passenger.impacted_at:
            keys += [(OFFERS, f"{passenger.key}-O1"), (OFFERS, f"{passenger.key}-O2")]
    return keys


MISSED_MIDNIGHTS = 3  # a starting stream tombstones the days the last this many midnights would have


def startup_tombstones(seed: int, now: datetime) -> list[tuple[str, str, None]]:
    """Tombstones for the days past retention at today's and the two previous midnights.

    The stream tombstones a day only at a midnight it streams through, so a generator down at midnight would skip
    it forever. Republishing a tombstone is harmless, so every start sends these. (The day after next needs no
    catch-up: continuous_state always covers tomorrow.)"""
    rows = []
    for k in range(MISSED_MIDNIGHTS):
        expired = now.date() - timedelta(days=RETENTION_DAYS + k)
        rows += [(topic, key, None) for topic, key in _expired_keys(seed, expired)]
    return rows


def continuous_events(seed: int, start: datetime, end: datetime) -> list[tuple[datetime, str, str, dict | None]]:
    """Timed updates in (start, end]: flight ticks, hotel rooms, and at each UTC midnight the day after next's
    schedule and passengers plus tombstones for the day past RETENTION_DAYS. Offers come from the agent."""
    first = start.date() - timedelta(days=1)
    days = [continuous_day(seed, first + timedelta(days=n)) for n in range((end.date() - first).days + 2)]
    events = [
        (tick, FLIGHTS, flight.key, flight.row(tick))
        for day in days
        for flight in day.flights
        for tick in flight.ticks(start, end)
    ]
    for n in range((end.date() - start.date()).days + 1):
        service_date = start.date() + timedelta(days=n)
        rows, times = continuous_hotel_rooms(seed, service_date), _continuous_room_times(seed, service_date)
        # A day's last row is stamped the next midnight, when the next day refills the hotels instead.
        for at, hotel, rooms in rows[bisect_right(times, start) : bisect_right(times, end)]:
            if at.date() == service_date:
                events.append((at, HOTELS, hotel, _hotel(hotel, rooms)))
    midnight = _midnight(start.date() + timedelta(days=1))
    while midnight <= end:
        rooms = rooms_at(seed, midnight)
        events += [(midnight, HOTELS, name, _hotel(name, rooms[name])) for name in HOTEL_ORDER]
        upcoming = continuous_day(seed, midnight.date() + timedelta(days=1))
        events += [(midnight, FLIGHTS, f.key, f.row(midnight)) for f in upcoming.flights]
        events += [(midnight, ITINERARIES, p.key, _itinerary(p)) for p in upcoming.passengers]
        expired = midnight.date() - timedelta(days=RETENTION_DAYS)
        events += [(midnight, topic, key, None) for topic, key in _expired_keys(seed, expired)]
        midnight += timedelta(days=1)
    events.sort(key=itemgetter(0))
    return events


def continuous_history(seed: int, now: datetime) -> list[tuple[str, str, dict]]:
    """The finished days before yesterday (D-30 to D-2): final flight rows, connecting passengers, completed offers."""
    today = now.date()
    dates = [today - timedelta(days=back) for back in range(HISTORY_DAYS, 1, -1)]

    def build(service_date: date) -> Day:  # uncached, so 29 old days don't push the live ones out of the cache
        return continuous_day.__wrapped__(seed, service_date)

    rows = []
    day = build(dates[0])
    for service_date in dates:
        following = service_date + timedelta(days=1)
        next_day = continuous_day(seed, following) if following == today - timedelta(days=1) else build(following)
        rows += [(FLIGHTS, f.key, f.row(datetime.max)) for f in day.flights]
        rows += [(ITINERARIES, p.key, _itinerary(p)) for p in day.passengers if p.connecting]
        for recovery in _recoveries(day, next_day, seed, None):
            rows += [(OFFERS, key, value) for key, value in recovery.rows(datetime.max)]
        day = next_day
    return rows


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
        rooms = {
            name: min(full, max(1, rooms[name] + rng.choices((-1, 0, 1), (20, 50, 30))[0]))
            for name, full in (
                (GRAND_HYATT, GRAND_HYATT_ROOMS),
                (MARRIOTT, MARRIOTT_ROOMS * 2),
                (HILTON, HILTON_ROOMS * 2),
            )
        }
    for step in reversed(steps):
        for name in HOTEL_ORDER:
            yield HOTELS, name, _hotel(name, step[name])


def _itinerary(passenger: Passenger) -> dict:
    return dict(
        inbound_flight_id=passenger.inbound.key,
        inbound_seat=passenger.inbound_seat,
        connecting_flight_id=passenger.connecting.key if passenger.connecting else None,
        connecting_seat=passenger.connecting_seat,
    )


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
                yield ITINERARIES, passenger.key, _itinerary(passenger)
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


FLUSH_ROUND_SECONDS = 30  # Publisher.flush waits this long per round
FLUSH_STALLED_ROUNDS = 3  # ...and gives up after this many rounds in a row that deliver nothing
FLUSH_LIMIT = timedelta(minutes=15)  # ...or after this long in all


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
        self.webhook_auth = (
            credentials.get("hotel_webhook_api_key", ""),
            credentials.get("hotel_webhook_api_secret", ""),
        )
        self.schema_registry = SchemaRegistryClient(
            {
                "url": credentials["schema_registry_url"],
                "basic.auth.user.info": (
                    f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}"
                ),
            }
        )
        self.producer = Producer(
            {
                "bootstrap.servers": credentials["bootstrap_servers"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": credentials["kafka_api_key"],
                "sasl.password": credentials["kafka_api_secret"],
                "linger.ms": 20,
            }
        )

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
                AvroSerializer(
                    self.schema_registry,
                    registered.schema.schema_str,
                    conf={"auto.register.schemas": False, "use.latest.version": True},
                ),
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
            time.sleep(2**attempt)  # a retry can duplicate a record; hotel_inventory keeps only the latest

    def flush(self) -> None:
        """Wait until every record is delivered, for as long as delivery keeps making progress.

        A month of history from a laptop takes minutes to drain. Give up only when FLUSH_STALLED_ROUNDS rounds in a
        row deliver nothing, or after FLUSH_LIMIT in all."""
        if self.dry_run:
            return
        deadline = time.monotonic() + FLUSH_LIMIT.total_seconds()
        last, stalled = None, 0
        while True:
            pending = self.producer.flush(FLUSH_ROUND_SECONDS)
            if not pending:
                return
            stalled = stalled + 1 if last is not None and pending >= last else 0
            if stalled >= FLUSH_STALLED_ROUNDS or time.monotonic() >= deadline:
                raise RuntimeError(f"{pending} records were not delivered")
            logging.info("%d records still to deliver", pending)
            last = pending


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
            command = subprocess.run(
                ["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True, check=False
            ).stdout
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


def run(
    start: datetime,
    seed: int = SEED,
    history: bool = True,
    minutes: int = STREAM_MINUTES,
    dry_run: bool = False,
    stream_only: bool = False,
    agent_offers: bool = False,
    speed: float = 1.0,
) -> None:
    plan = build_plan(start, seed, history=history and not stream_only)
    if agent_offers:
        agent = [f.key for f in plan.live.agent_flights]
        plan.recoveries = [r for r in plan.recoveries if r.passenger.inbound.key not in agent]
        logging.info("The recovery agent writes the offers for %s; the generator writes the rest", ", ".join(agent))
    publisher = Publisher(None if dry_run else extract_kafka_credentials("aws", get_project_root()), dry_run)
    if not dry_run:
        stop_previous_stream()
    if not stream_only:
        count = 0
        for topic, key, value in initial_records(plan):
            publisher.publish(topic, key, value)
            count += 1
        publisher.flush()
        logging.info("Published %d records: %d history days, today, and tomorrow's schedule", count, len(plan.days) - 1)
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
    logging.info(
        "Streaming %d updates for %d minutes (%.4g minutes of waiting at %gx); %s is scheduled at %s UTC",
        len(events),
        minutes,
        minutes / speed,
        speed,
        hero.key,
        f"{hero.scheduled:%H:%M}",
    )
    follow = passenger_to_follow(plan)
    logging.info(
        "Passenger to follow: %s, seat %s on %s, connecting to %s for %s",
        follow.key,
        follow.inbound_seat,
        hero.key,
        follow.connecting.key,
        follow.connecting.destination,
    )
    end = plan.start + timedelta(minutes=minutes)
    moments = [(f"{GRAND_HYATT} sells out", plan.start + SELLOUT)]
    for flight in plan.live.agent_flights:
        impacted = [p.impacted_at for p in plan.live.passengers if p.inbound is flight and p.impacted_at]
        moments += [
            (f"{flight.key}'s first passenger turns HIGH", min(impacted)),
            (f"{flight.key}'s last passenger turns HIGH", max(impacted)),
        ]
    for moment, at in sorted(moments, key=lambda moment: moment[1]):
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


CONTINUOUS_WINDOW = timedelta(hours=1)  # the continuous stream works out its events an hour at a time
WATCH_EVERY = 1.0  # seconds between offer polls while the stream waits


def _wall_clock() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _stream_continuous(seed: int, start: datetime, until: datetime | None, publish, watcher=None, clock=None) -> None:
    """Publish continuous_events after `start` (until `until`, or forever), plus the customers' answers to offers.

    `clock` returns the current UTC time; without one nothing waits. The watcher's rows join the pending heap.
    Right before a customer's first row goes out, the watcher must still see the offers it answered open and
    unchanged: if someone booked in the app first, or the app rewrote an offer, every pending row for that
    passenger is dropped.
    """
    pending: list[tuple[datetime, int, str, str, dict | None, str | None]] = []
    order = count()
    loaded = start
    approved: set[str] = set()
    dropped: set[str] = set()
    minute, sent = None, 0

    def push(rows) -> None:
        for at, key, value in rows:
            heapq.heappush(pending, (at, next(order), OFFERS, key, value, value["passenger_id"]))

    while True:
        # Keep the next hour of events on hand, so a customer row due later never jumps ahead of them.
        while (until is None or loaded < until) and (not pending or pending[0][0] > loaded):
            window_end = loaded + CONTINUOUS_WINDOW if until is None else min(loaded + CONTINUOUS_WINDOW, until)
            for at, topic, key, value in continuous_events(seed, loaded, window_end):
                heapq.heappush(pending, (at, next(order), topic, key, value, None))
            loaded = window_end
        if not pending or (until is not None and pending[0][0] > until):
            break
        at = pending[0][0]
        wait = (at - clock()).total_seconds() if clock else 0.0
        if watcher is not None:
            rows = watcher.poll(max(0.0, min(wait, WATCH_EVERY)))
            push(rows)
            if rows or (clock and at > clock()):
                continue  # look again: a customer row may be due first, and the poll may not have waited enough
        elif wait > 0:
            time.sleep(wait)
        at, _, topic, key, value, passenger = heapq.heappop(pending)
        if passenger is not None:
            if passenger in dropped:
                continue
            if passenger not in approved:
                if not watcher.still_open(passenger):
                    dropped.add(passenger)
                    logging.info("%s already decided in the app; dropping the simulated answer", passenger)
                    continue
                approved.add(passenger)  # our own first row closes the offers, so check only once
        publish(topic, key, value)
        sent += 1
        if minute != at.replace(second=0, microsecond=0):
            if minute is not None:
                logging.info("%s UTC: %d updates", f"{minute:%H:%M}", sent)
            minute, sent = at.replace(second=0, microsecond=0), 0


def run_continuous(
    seed: int = SEED,
    now: datetime | None = None,
    history: bool = False,
    dry_run: bool = False,
    until: datetime | None = None,
) -> None:
    """The always-on stream: publish the state as of `now` (and history), then stream on the wall clock forever.

    The state depends only on the seed and the clock, so a restart picks up where the last stream was.
    """
    now = now or _wall_clock().replace(microsecond=0)
    if dry_run and until is None:
        until = now + CONTINUOUS_WINDOW  # a dry run prints an hour rather than forever
    credentials = None if dry_run else extract_kafka_credentials("aws", get_project_root())
    publisher = Publisher(credentials, dry_run)
    if not dry_run:
        stop_previous_stream()
    if history:
        rows = continuous_history(seed, now)
        for topic, key, value in rows:
            publisher.publish(topic, key, value)
        publisher.flush()
        logging.info("Published %d history records for the %d days before yesterday", len(rows), HISTORY_DAYS - 1)
    rows = continuous_state(seed, now)
    for topic, key, value in rows:
        publisher.publish(topic, key, value)
    publisher.flush()
    logging.info("Published %d records: the hotels, yesterday, today, and tomorrow as of %s UTC", len(rows), now)
    if not dry_run:  # a dry run prints the live rows only
        tombstones = startup_tombstones(seed, now)
        for topic, key, value in tombstones:
            publisher.publish(topic, key, value)
        publisher.flush()
        logging.info("Published %d tombstones for the days past retention", len(tombstones))
    if until is not None and until <= now:
        return
    watcher = None
    pid_file = _pid_file()
    if not dry_run:
        from .offer_customers import OfferWatcher  # it imports this module

        watcher = OfferWatcher(credentials, seed)
        pid_file.parent.mkdir(exist_ok=True)
        pid_file.write_text(str(os.getpid()))
    logging.info("Streaming continuously%s", f" until {until} UTC" if until else "; Ctrl-C stops it")
    try:
        _stream_continuous(seed, now, until, publisher.publish, watcher, None if dry_run else _wall_clock)
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
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="stream around the clock: every flight's state follows the seed and the UTC wall clock",
    )
    parser.add_argument(
        "--history", action="store_true", help=f"with --continuous, first publish {HISTORY_DAYS - 1} finished days"
    )
    parser.add_argument("--until", help=argparse.SUPPRESS)  # with --continuous: stop streaming at this UTC time
    parser.add_argument(
        "--minutes",
        type=int,
        default=None,
        help=f"minutes of live updates to stream (default: {STREAM_MINUTES}; 0 exits after publishing)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=None,
        help="play the stream this many times faster, e.g. 5 runs 90 minutes in 18 (default: 1). "
        "Event timestamps keep stream time, so the dashboard's clock runs ahead of the wall clock",
    )
    parser.add_argument("--skip-history", action="store_true", help=f"skip the {HISTORY_DAYS} days of history")
    parser.add_argument(
        "--stream-only", action="store_true", help="stream the live updates for an earlier --now without republishing"
    )
    parser.add_argument("--dry-run", action="store_true", help="print records as JSON; no Kafka writes, no waiting")
    parser.add_argument("--reset", action="store_true", help="write tombstones for every generated key")
    parser.add_argument(
        "--offers",
        choices=("auto", "agent", "generator"),
        default=None,
        help="who writes the offers for RA417, RA183, and RA185 (default auto: the agent when Terraform runs it)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="pick the last stream back up where its clock is now, at its start and speed, "
        "without republishing; with --offers generator, the agent flights' missed offers arrive at once",
    )
    args = parser.parse_args(argv)
    if args.continuous:
        scripted = {
            "--minutes": args.minutes is not None,
            "--speed": args.speed is not None,
            "--resume": args.resume,
            "--stream-only": args.stream_only,
            "--offers": args.offers is not None,
            "--reset": args.reset,
            "--skip-history": args.skip_history,
        }
        given = [flag for flag, used in scripted.items() if used]
        if given:
            parser.error(f"--continuous can't be combined with {', '.join(given)}")
        setup_logging()
        now = _utc(args.now) if args.now else None
        try:
            run_continuous(args.seed, now, args.history, args.dry_run, _utc(args.until) if args.until else None)
        except KeyboardInterrupt:
            logging.info("Stopped")
        return
    if args.history or args.until:
        parser.error("--history and --until only apply with --continuous")
    args.minutes = STREAM_MINUTES if args.minutes is None else args.minutes
    args.speed = 1.0 if args.speed is None else args.speed
    args.offers = args.offers or "auto"
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
            run(
                start,
                args.seed,
                history=not args.skip_history,
                minutes=args.minutes,
                dry_run=args.dry_run,
                stream_only=args.stream_only,
                agent_offers=agent,
                speed=args.speed,
            )
    except KeyboardInterrupt:
        logging.info("Stopped")


if __name__ == "__main__":
    main()
