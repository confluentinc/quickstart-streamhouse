# Always-on Flight Disruptions Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One always-on Flight Disruptions deployment. A continuous generator and the app run on AWS,
the recovery agent writes every live offer, and six demo users have full access to the environment.

**Architecture:**
- **Generator:** `scripts/airport_datagen.py` gains an around-the-clock schedule and a `--continuous`
  mode. The mode works out every flight's state from the seed and the wall clock, streams forever,
  rolls days over at UTC midnight, and plays the customer for the agent's offers.
- **App:** `scripts/airport_app.py` and the generator can read their credentials from environment
  variables, and the app gains optional HTTP Basic auth.
- **AWS:** a new `terraform/always-on/` root runs one container image twice: as an App Runner service
  (the app) and an ECS Fargate service (the generator). The same root also invites the demo users.
- **Deploy:** `uv run deploy --always-on` wires it all together.

**Tech Stack:** Python 3.12 with `uv`, FastAPI, confluent-kafka (Avro), Terraform (confluent
`~> 2.38`, hashicorp/aws `>= 5.17`, hashicorp/random), Docker, AWS App Runner / ECS Fargate / ECR /
Secrets Manager / CloudWatch Logs.

**Spec:** [`docs/superpowers/specs/2026-11-12-continuous-demo-on-aws-design.md`](../specs/2026-11-12-continuous-demo-on-aws-design.md)

## Global Constraints

- `uv` for everything Python; never bare `pip`.
- `uv run pytest` and `uv run ruff check scripts tests` pass after every task (line length 120).
- The scripted 90-minute mode, the workshop, and default `uv run deploy` behave exactly as before:
  every existing test passes unchanged.
- Every generator path takes a seed and a controllable clock (`--now`). Continuous mode has no state
  file.
- 180 flights per service day (90 arrivals, 90 departures), the 182-seat `SEATS` layout.
- No credentials, passwords, or demo users' email addresses in tracked files. Secrets live in
  `credentials.env` (gitignored), `tmp/` (gitignored), Terraform state (gitignored), or AWS Secrets
  Manager.
- Business-readable resource names. Files kebab-case, Python modules snake_case.
- Markdown links between local files are relative.
- Push with `git push-external`, never plain `git push`.

## Review Focus

1. **A generator restart mid-day** must not rewind any flight, re-send already-published offers, or
   flip a passenger back to OK. It republishes the current state and carries on.
   → Task 5 `test_restart_republishes_the_same_state`.
2. **Midnight UTC rollover.** Flights scheduled late on day D tick into D+1, and day D+1's flights start
   ticking at 21:00 on D. Both days must stream with no gap or duplicates.
   → Task 5 `test_events_cross_midnight_without_gaps`.
3. **A colleague books in the app before the simulated customer does.** The simulator must leave that
   passenger alone, not overwrite BOOKED with CLOSED.
   → Task 4 `test_human_decision_wins`.
4. **The agent leaves O2 empty** (`recommended_flight_id` null when it found only one flight). The
   simulator books O1 or lets it expire. It never books an offer that has no flight.
   → Task 4 `test_never_books_an_offer_without_a_flight`.
5. **A colleague opens the board at any UTC hour** and sees delayed flights in the app's 24-hour
   window, including one RA417 that is delayed and still in the air.
   → Task 2 `test_board_shows_delays_every_hour` and `test_an_ra417_is_delayed_in_the_air_at_every_minute`.

---

### Task 0: Plus-addressed invitations (done 2026-11-12)

The operator invited the six demo addresses (plus-addressed aliases of one mailbox, kept in
`credentials.env`) into the deploying organization with `confluent iam user invitation create`. Each
address got its own user ID, and the first one accepted and signed in as a separate Username/Password
user, not as the mailbox's own account. Nothing
here invites users any more. Task 10 only grants their environment role.

---

### Task 1: Around-the-clock schedule

The schedule becomes a parameter. The scripted template is unchanged; continuous mode adds a 24-hour
template.

**Files:**
- Modify: `scripts/airport_datagen.py` (constants near line 64, `Day`, `build_day`, `_assign_delays`,
  `_passengers`, `_revise_estimates`, `build_plan`)
- Test: `tests/test_continuous.py` (new)

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Schedule: arrivals: tuple[int, ...]; departures: tuple[int, ...]; scripted: bool; variant: str`
  - `SCRIPTED = Schedule(ARRIVAL_MINUTES, DEPARTURE_MINUTES, True, "")`
  - `AROUND_THE_CLOCK = Schedule(CONTINUOUS_ARRIVAL_MINUTES, CONTINUOUS_DEPARTURE_MINUTES, False, "continuous")`
  - `Day.schedule: Schedule = SCRIPTED`. `Day.hero` raises `ValueError` when `not schedule.scripted`.
    `Day.agent_flights` returns `[]` when `not schedule.scripted`.
  - `CONTINUOUS_HERO_MINUTES = tuple(40 + 180 * k for k in range(8))`: 00:40, 03:40, … 21:40.
  - `Day.heroes -> list[Flight]`: the RA417s on an `AROUND_THE_CLOCK` day, or `[self.hero]` on a scripted
    day.
  - Hero IDs on `AROUND_THE_CLOCK` days: flight key `f"RA417-{date:%Y%m%d}-{minute // 60:02d}{minute % 60:02d}"`
    and passenger key `f"P-{date:%m%d}-417-{hhmm}-{seq:03d}"`. Every other key keeps today's format.
  - `build_day(seed, service_date, midnight, live=False, schedule_only=False, schedule=SCRIPTED) -> Day`
  - `_revise_estimates(day: Day, rng: random.Random, start: datetime, end: datetime) -> None`
    (`build_plan` passes the scripted window it computes today).

- [ ] **Step 1: Write the failing tests** in `tests/test_continuous.py`:

```python
from datetime import date, datetime, timedelta
from scripts import airport_datagen as g

DAY = date(2026, 11, 12)
MIDNIGHT = datetime(2026, 11, 12)

def continuous_day():
    return g.build_day(g.SEED, DAY, MIDNIGHT, live=True, schedule=g.AROUND_THE_CLOCK)

def test_around_the_clock_has_180_flights_spread_over_24_hours():
    day = continuous_day()
    assert len(day.arrivals) == 90 and len(day.departures) == 90
    hours = {f.scheduled.hour for f in day.flights}
    assert hours == set(range(24))
    assert all(MIDNIGHT <= f.scheduled < MIDNIGHT + timedelta(days=1) for f in day.flights)

def test_each_destination_has_one_overnight_gap_staggered_across_the_day():
    day = continuous_day()
    starts = []
    for spoke in g.SPOKES:
        times = sorted(f.scheduled for f in day.departures if f.destination == spoke)
        gaps = [(b - a, a) for a, b in zip(times, times[1:] + [times[0] + timedelta(days=1)])]
        long = [a for gap, a in gaps if gap > g.OVERNIGHT]
        assert len(long) == 1
        starts.append(long[0].hour)
    assert len(set(starts)) >= 8  # staggered, not all at the same hour

def test_continuous_days_have_eight_ra417s_from_chicago_every_three_hours():
    day = continuous_day()
    heroes = day.heroes
    assert [f.number for f in heroes] == [g.HERO_NUMBER] * 8
    assert all(f.origin == g.HERO_ORIGIN and f.destination == g.HUB for f in heroes)
    assert [f.scheduled for f in heroes] == [MIDNIGHT + timedelta(minutes=m) for m in g.CONTINUOUS_HERO_MINUTES]
    assert [f.key for f in heroes][0] == "RA417-20261112-0040"
    assert len({f.key for f in day.flights}) == 180
    assert day.agent_flights == []

def test_scripted_schedule_is_unchanged():
    day = g.build_day(g.SEED, DAY, MIDNIGHT, live=True)
    assert day.schedule is g.SCRIPTED and day.hero.number == g.HERO_NUMBER
```

- [ ] **Step 2: Run them to make sure they fail.**
  Run: `uv run pytest tests/test_continuous.py -v`. Expected: FAIL with `AttributeError: ... 'AROUND_THE_CLOCK'`.

- [ ] **Step 3: Implement the schedule.**
  - **Arrivals:** `CONTINUOUS_ARRIVAL_MINUTES = tuple(i * 16 for i in range(90))`. Arrival `i`'s
    origin stays `SPOKES[i % 15]`, as `build_day` does today.
  - **Departures:** departure `i` keeps destination `SPOKES[i % 15]`. For destination `d = i % 15` and
    its `k = i // 15`-th departure (k = 0…5), the minute is
    `(gap_end(d) + k * 204) % 1440`, where `gap_end(d) = (d * 96 + 420) % 1440`. That gives a 7-hour gap
    starting at `d * 96`, then 6 departures 3 h 24 m apart. Sort nothing: index order carries the
    destination.
  - **`build_day`:** with `scripted=False`, it turns the arrival slot nearest each
    `CONTINUOUS_HERO_MINUTES` entry into an RA417 from ORD scheduled exactly at that minute, so there are
    still 90 arrivals. It numbers the other arrivals from 101 upward (skipping 417) and the departures
    601–690, and seeds its RNG with `f"{seed}:{date}:{schedule.variant or variant}"`.
  - **`_assign_delays` and `_passengers`:** they take the hero, scripted, and forced last-bank branches
    only when `day.schedule.scripted`.

- [ ] **Step 4: Run the whole suite.**
  Run: `uv run pytest -q`. Expected: all pass. In particular, `tests/test_flight_recovery.py`'s
  `test_same_seed_same_data` shows the scripted output didn't change.

- [ ] **Step 5: Commit.**
  `git add scripts/airport_datagen.py tests/test_continuous.py && git commit -m "Add an around-the-clock flight schedule to the generator"`

---

### Task 2: Continuous days behave like today's live day

Continuous days need realistic delays, connections, and hotel offers.

**Files:**
- Modify: `scripts/airport_datagen.py`
- Test: `tests/test_continuous.py`

**Interfaces:**
- Consumes: `build_day(..., schedule=AROUND_THE_CLOCK)` and `_revise_estimates(day, rng, start, end)`
  from Task 1.
- Produces:
  - `CONTINUOUS_HERO_RAMP = (timedelta(minutes=-75), timedelta(minutes=-25))`: relative to each RA417's
    scheduled arrival, its delay climbs with every update from the first to the second, up to `HERO_DELAY`
    (115). Each RA417 has `HERO_CONNECTING` (150) connecting and `HERO_LOCAL` (20) local passengers,
    and every connecting one turns HIGH. Its passengers' onward departures come from the same day or
    the next, and must not run out of seats.
  - `continuous_day(seed: int, service_date: date) -> Day`. It is cached
  (`functools.lru_cache`), uses midnight = `datetime.combine(service_date, time())` (UTC), `live=True`,
  `AROUND_THE_CLOCK`, and applies `_revise_estimates` over the whole day
  (`[midnight - ACTIVE_BEFORE, midnight + 1 day]`) with RNG `f"{seed}:{date}:continuous:revisions"`.

- [ ] **Step 1: Write the failing tests:**

```python
def test_continuous_delay_and_risk_rates_match_today():
    days = [g.continuous_day(g.SEED, DAY + timedelta(days=n)) for n in range(7)]
    for day in days:
        heroes = set(map(id, day.heroes))
        others = [f for f in day.arrivals if id(f) not in heroes]
        delayed = sum(1 for f in others if f.delay_at(datetime.max) >= g.DELAYED_AT)
        high = sum(1 for p in day.passengers if p.impacted_at and id(p.inbound) not in heroes)
        assert 15 <= delayed <= 45  # the random delays, besides the eight RA417s
        assert 300 <= high <= 900

def test_hotel_offers_stay_about_as_common_as_today():
    def hotel_share(day, next_day):
        recoveries = g._recoveries(day, next_day, g.SEED, None)
        return sum(1 for r in recoveries if any(r.hotels)) / len(recoveries)
    scripted = g.build_plan(datetime(2026, 11, 12, 17, 0))
    baseline = hotel_share(scripted.days[-2], scripted.days[-1])
    share = hotel_share(g.continuous_day(g.SEED, DAY), g.continuous_day(g.SEED, DAY + timedelta(days=1)))
    assert baseline / 2 <= share <= baseline * 2

def test_every_ra417_strands_150_of_170_passengers():
    day = g.continuous_day(g.SEED, DAY)
    for hero in day.heroes:
        riders = [p for p in day.passengers if p.inbound is hero]
        assert len(riders) == 170
        assert sum(1 for p in riders if p.impacted_at) == 150
        assert hero.delay_at(datetime.max) == g.HERO_DELAY

def test_connecting_seats_never_run_out():
    for n in range(3):
        day = g.continuous_day(g.SEED, DAY + timedelta(days=n))
        seats = [(p.connecting.key, p.connecting_seat) for p in day.passengers if p.connecting]
        assert len(seats) == len(set(seats))
        assert all(seat in g.SEATS for _, seat in seats)

def test_an_ra417_is_delayed_in_the_air_at_every_minute():
    days = [g.continuous_day(g.SEED, DAY + timedelta(days=n)) for n in (-1, 0, 1)]
    heroes = [f for d in days for f in d.heroes]
    for minute in range(0, 24 * 60, 5):
        now = MIDNIGHT + timedelta(minutes=minute)
        assert any(f.delay_at(now) >= g.DELAYED_AT and f.status_at(now) == "DELAYED" for f in heroes)

def test_board_shows_delays_every_hour():
    days = {n: g.continuous_day(g.SEED, DAY + timedelta(days=n)) for n in (-1, 0, 1)}
    flights = [f for d in days.values() for f in d.flights]
    for hour in range(24):
        now = MIDNIGHT + timedelta(hours=hour)
        window = [f for f in flights if now - timedelta(hours=18) <= f.scheduled < now + timedelta(hours=6)]
        assert sum(1 for f in window if f.delay_at(now) >= g.DELAYED_AT) >= 3

def test_continuous_day_is_deterministic():
    g.continuous_day.cache_clear()
    a = g.continuous_day(g.SEED, DAY)
    g.continuous_day.cache_clear()
    b = g.continuous_day(g.SEED, DAY)
    assert [f.reveals for f in a.flights] == [f.reveals for f in b.flights]
    assert [(p.key, p.connecting and p.connecting.key) for p in a.passengers] == [
        (p.key, p.connecting and p.connecting.key) for p in b.passengers
    ]
```

- [ ] **Step 2: Run them to make sure they fail.**
  Run: `uv run pytest tests/test_continuous.py -v`. Expected: FAIL (`continuous_day` not defined).

- [ ] **Step 3: Implement `continuous_day`.** If `test_board_shows_delays_every_hour` or the hotel-share
  test fails, tune only continuous-mode constants, adding `CONTINUOUS_` copies where needed. Don't
  change the scripted constants. Late-evening arrivals with no same-day departures in reach may connect
  to the next day's departures; that's the builder's call, as long as the tests pass.

- [ ] **Step 4: Run the suite.** Run: `uv run pytest -q`. Expected: all pass.

- [ ] **Step 5: Commit.** `git commit -am "Model continuous service days at today's delay and hotel rates"`

---

### Task 3: Continuous hotel rooms

The hotels need a daily cycle: the Grand Hyatt sells out and comes back.

**Files:**
- Modify: `scripts/airport_datagen.py`
- Test: `tests/test_continuous.py`

**Interfaces:**
- Produces:
  - `continuous_hotel_rooms(seed: int, service_date: date) -> list[tuple[datetime, str, int]]`: every
    `HOTEL_EVERY` from midnight to midnight, for all three hotels, in time order, cached.
  - `rooms_at(seed: int, at: datetime) -> dict[str, int]`: the latest value at or before `at`.
  - Constant `CONTINUOUS_SELLOUT_HOUR = 18`: the Grand Hyatt drains from `GRAND_HYATT_ROOMS` to 0 by
    18:00 UTC, stays at 0 until midnight, and refills at 00:00. Marriott and Hilton random-walk the way
    `_hotel_rooms` does, starting each day from their full counts, seeded `f"{seed}:{date}:continuous:hotels"`.

- [ ] **Step 1: Write the failing tests:**

```python
def test_grand_hyatt_sells_out_each_evening_and_refills_at_midnight():
    assert g.rooms_at(g.SEED, MIDNIGHT + timedelta(hours=19))[g.GRAND_HYATT] == 0
    assert g.rooms_at(g.SEED, MIDNIGHT + timedelta(minutes=1))[g.GRAND_HYATT] > 0
    assert all(r >= 1 for h, r in g.rooms_at(g.SEED, MIDNIGHT + timedelta(hours=19)).items() if h != g.GRAND_HYATT)

def test_hotel_rooms_publish_every_15_seconds():
    rows = g.continuous_hotel_rooms(g.SEED, DAY)
    assert len(rows) == 3 * (24 * 3600 // 15)
```

- [ ] **Step 2: Run them to make sure they fail.** Run: `uv run pytest tests/test_continuous.py -v`.
- [ ] **Step 3: Implement both functions.**
- [ ] **Step 4: Run the suite.** Run: `uv run pytest -q`. Expected: all pass.
- [ ] **Step 5: Commit.** `git commit -am "Give continuous hotels a daily sellout cycle"`

---

### Task 4: Simulated customers for the agent's offers

The generator plays the customer for each offer the agent writes. The decision logic is pure; a thin
Kafka consumer feeds it.

**Files:**
- Create: `scripts/offer_customers.py`
- Test: `tests/test_offer_customers.py`

**Interfaces:**
- Consumes: `BOOKING_SHARE`, `DECISION_SECONDS`, `OFFER_EXPIRY`, `OFFERS`, and `rooms_at` from
  `airport_datagen`.
- Produces:
  - `decide(seed: int, offers: dict[str, dict]) -> list[tuple[datetime, str, dict]]`. `offers` maps
    offer keys (`P-…-O1`, `P-…-O2`) to their current values, with field names as in
    `Recovery.rows`. It returns the timed rows to publish: BOOKED for the chosen offer and CLOSED for the
    other at `decided_at`, or CLOSED for both at `recommended_at + OFFER_EXPIRY`. It returns `[]` when
    any offer's status isn't `OFFERED`.
    - **Seeding:** `random.Random(f"{seed}:{passenger_id}:customer")`.
    - **Draws:** books with probability `BOOKING_SHARE`, then `decided_at = recommended_at +
      randint(*DECISION_SECONDS)` seconds, then choice O1 with probability 0.7.
    - **Hotel check:** if the chosen offer's hotel has 0 rooms at `decided_at` (`rooms_at`), it switches
      to the other offer.
    - **Missing flights:** an offer with `recommended_flight_id` null is never chosen.
  - `class OfferWatcher` with:
    - `__init__(self, credentials: dict, seed: int)`: a Consumer on `OFFERS`, a unique `group.id`,
      `auto.offset.reset=earliest`, `enable.auto.commit=False`, and an Avro deserializer.
    - `poll(self, timeout: float) -> list[tuple[datetime, str, dict]]`: updates its per-passenger view
      and returns `decide()` for passengers whose two offers are now both present and OFFERED, each
      passenger at most once per process.

- [ ] **Step 1: Write the failing tests** in `tests/test_offer_customers.py`. Build offer dicts with a
  helper `offer(passenger, n, flight="RA601-20261112", hotel=None, status="OFFERED", at=datetime(2026, 11, 12, 10))`.

```python
def test_about_85_percent_book_and_the_rest_expire():
    results = [decide(42, pair(f"P-1112-101-{i:03d}")) for i in range(1, 401)]
    booked = sum(any(v["status"] == "BOOKED" for _, _, v in rows) for rows in results)
    assert 300 <= booked <= 380
    expired = [rows for rows in results if not any(v["status"] == "BOOKED" for _, _, v in rows)]
    assert all(at == AT + OFFER_EXPIRY and v["status"] == "CLOSED" for rows in expired for at, _, v in rows)

def test_decision_is_seeded_by_passenger():
    assert decide(42, pair("P-1112-101-001")) == decide(42, pair("P-1112-101-001"))

def test_human_decision_wins():
    offers = pair("P-1112-101-001")
    offers["P-1112-101-001-O1"]["status"] = "SELECTED"
    assert decide(42, offers) == []

def test_never_books_an_offer_without_a_flight():
    for i in range(1, 200):
        offers = pair(f"P-1112-101-{i:03d}")
        offers[f"P-1112-101-{i:03d}-O2"]["recommended_flight_id"] = None
        booked = [k for _, k, v in decide(42, offers) if v["status"] == "BOOKED"]
        assert booked in ([], [f"P-1112-101-{i:03d}-O1"])

def test_sold_out_hotel_switches_to_the_other_offer():
    late = datetime(2026, 11, 12, 18, 30)  # after the Grand Hyatt's daily sellout, before midnight
    for i in range(1, 200):
        pid = f"P-1112-101-{i:03d}"
        offers = {
            f"{pid}-O1": offer(pid, 1, hotel=GRAND_HYATT, at=late),
            f"{pid}-O2": offer(pid, 2, at=late),
        }
        for at, key, value in decide(42, offers):
            if value["status"] == "BOOKED" and at.date() == late.date():
                assert key.endswith("-O2")
```

- [ ] **Step 2: Run them to make sure they fail.** Run: `uv run pytest tests/test_offer_customers.py -v`.
  Expected: FAIL (`ModuleNotFoundError`).
- [ ] **Step 3: Implement `decide` and `OfferWatcher`.** Build the consumer's credentials the way
  `Publisher.__init__` builds the producer's (SASL_SSL, PLAIN).
- [ ] **Step 4: Run the suite.** Run: `uv run pytest -q`. Expected: all pass.
- [ ] **Step 5: Commit.** `git add scripts/offer_customers.py tests/test_offer_customers.py && git commit -m "Simulate customers deciding on the agent's offers"`

---

### Task 5: The continuous stream and its CLI

This task turns Tasks 1–4 into a long-running stream with a command-line entry point.

**Files:**
- Modify: `scripts/airport_datagen.py` (new functions plus `main()`; update the module docstring's
  usage block)
- Test: `tests/test_continuous.py`

**Interfaces:**
- Consumes: `continuous_day`, `continuous_hotel_rooms`, `rooms_at` (Tasks 2–3), and `OfferWatcher`
  (Task 4).
- Produces:
  - `RETENTION_DAYS = 35`
  - `continuous_state(seed: int, now: datetime) -> list[tuple[str, str, dict]]`: the current hotel rows,
    every flight row as of `now` for days D-1, D, D+1, and every passenger itinerary for those days, in
    the same shape as `initial_records`.
  - `continuous_events(seed: int, start: datetime, end: datetime) -> list[tuple[datetime, str, str, dict | None]]`,
    sorted, covering (start, end]:
    - flight ticks for every flight of days D-1…D+1 that are active in the window;
    - hotel rows;
    - at each UTC midnight M in the window: flight rows and itineraries for day `M.date() + 1`, plus
      tombstones (`None`) for every flight, itinerary, and `-O1`/`-O2` offer key of day
      `M.date() - RETENTION_DAYS`.
  - `continuous_history(seed: int, now: datetime) -> list[tuple[str, str, dict]]`: final flight rows,
    connecting passengers' itineraries, and the generator's completed offers (`_recoveries`, all
    `completes=True`) for days D-30…D-2.
  - `run_continuous(seed: int, now: datetime | None = None, history: bool = False, dry_run: bool = False, until: datetime | None = None) -> None`:
    - **On start:** publishes `continuous_history` if `history`, then `continuous_state`.
    - **Loop:** streams `continuous_events` an hour at a time until `until`, or forever.
    - **Customers:** between events it polls `OfferWatcher` with a timeout no longer than the wait, and
      merges the returned rows into a heap of pending events.
    - **Dry run:** skips the watcher and prints JSON without waiting.
    - **Logging:** once a minute, the update count.
  - **CLI:** `uv run airport-datagen --continuous [--history] [--now ISO] [--seed N] [--dry-run]`.
    `--continuous` can't be combined with `--minutes`, `--speed`, `--resume`, `--stream-only`,
    `--offers`, or `--reset`. Without `--history` it publishes no history.

- [ ] **Step 1: Write the failing tests:**

```python
NOW = datetime(2026, 11, 12, 14, 7, 30)

STAGE = {"ON_TIME": 0, "DELAYED": 0, "BOARDING": 1, "DEPARTED": 2, "LANDED": 2}

def test_restart_republishes_the_same_state():
    assert g.continuous_state(g.SEED, NOW) == g.continuous_state(g.SEED, NOW)
    before = {k: v for t, k, v in g.continuous_state(g.SEED, NOW) if t == g.FLIGHTS}
    after = {k: v for t, k, v in g.continuous_state(g.SEED, NOW + timedelta(minutes=10)) if t == g.FLIGHTS}
    for key, row in before.items():
        assert STAGE[after[key]["status"]] >= STAGE[row["status"]]
        if row["status"] not in ("LANDED", "DEPARTED"):  # severe delays may shrink by at most 3 minutes
            assert after[key]["estimated_time"] >= row["estimated_time"] - timedelta(minutes=3)

def test_events_cross_midnight_without_gaps():
    start, end = datetime(2026, 11, 12, 23, 50), datetime(2026, 11, 13, 0, 10)
    events = g.continuous_events(g.SEED, start, end)
    ticks = {}
    for at, topic, key, value in events:
        # Streamed ticks only: day D+2's rows published at midnight and old-day tombstones aren't ticks.
        if topic == g.FLIGHTS and value is not None and not key.endswith("20261114"):
            ticks.setdefault(key, []).append(at)
    assert any(k.endswith("20261112") for k in ticks) and any(k.endswith("20261113") for k in ticks)
    assert all(b - a == g.UPDATE_EVERY for times in ticks.values() for a, b in zip(times, times[1:]))
    assert any(k.endswith("20261114") for _, t, k, _ in events if t == g.FLIGHTS)  # D+1 published at midnight
    assert len({(at, k) for at, _, k, _ in events}) == len(events)

def test_midnight_tombstones_the_day_past_retention():
    events = g.continuous_events(g.SEED, datetime(2026, 11, 12, 23, 59), datetime(2026, 11, 13, 0, 1))
    old = (date(2026, 11, 13) - timedelta(days=g.RETENTION_DAYS)).strftime("%Y%m%d")
    assert any(t == g.FLIGHTS and k.endswith(old) and v is None for _, t, k, v in events)

def test_continuous_writes_no_live_offers():
    events = g.continuous_events(g.SEED, NOW, NOW + timedelta(hours=1))
    assert not any(t == g.OFFERS and v is not None for _, t, _, v in events)

def test_history_is_finished_and_has_completed_offers():
    rows = g.continuous_history(g.SEED, NOW)
    offers = [v for t, _, v in rows if t == g.OFFERS]
    assert offers and {v["status"] for v in offers} <= {"BOOKED", "CLOSED"}
    assert all(v["estimated_time"] < NOW for t, _, v in rows if t == g.FLIGHTS)

def test_continuous_dry_run_prints_json(capsys):
    g.main(["--continuous", "--dry-run", "--now", "2026-11-12T14:00:00Z", "--until", "2026-11-12T14:02:00Z"])
    assert '"topic": "flight_status"' in capsys.readouterr().out

def test_continuous_rejects_scripted_flags():
    with pytest.raises(SystemExit):
        g.main(["--continuous", "--minutes", "5"])
```

  (Add a hidden `--until` flag, `argparse.SUPPRESS`, for the dry-run test.)

- [ ] **Step 2: Run them to make sure they fail.** Run: `uv run pytest tests/test_continuous.py -v`.
- [ ] **Step 3: Implement the functions and CLI.**
- [ ] **Step 4: Run the suite and lint.**
  Run: `uv run pytest -q && uv run ruff check scripts tests`. Expected: all pass.
- [ ] **Step 5: Commit.** `git commit -am "Add a continuous generator mode that streams forever"`

---

### Task 6: Credentials from environment variables

The app and generator must run on AWS, where there is no `credentials.env` or Terraform state.

**Files:**
- Modify: `scripts/terraform.py` (`extract_kafka_credentials`), `scripts/airport_app.py`
  (`Deployment._load_rtce`)
- Test: `tests/test_app.py`, `tests/test_continuous.py`

**Interfaces:**
- Produces:
  - `ENV_CREDENTIALS = {"bootstrap_servers": "STREAMHOUSE_BOOTSTRAP_SERVERS", "kafka_api_key": "STREAMHOUSE_KAFKA_API_KEY", "kafka_api_secret": "STREAMHOUSE_KAFKA_API_SECRET", "schema_registry_url": "STREAMHOUSE_SCHEMA_REGISTRY_URL", "schema_registry_api_key": "STREAMHOUSE_SCHEMA_REGISTRY_API_KEY", "schema_registry_api_secret": "STREAMHOUSE_SCHEMA_REGISTRY_API_SECRET"}`
    in `scripts/terraform.py`.
  - `credentials_from_env() -> dict[str, str] | None`: returns the credentials when every variable is
    set, and `None` otherwise. When all are set, `extract_kafka_credentials` returns them before looking
    for state. The other keys (`environment_name`, …) default to `""`.
  - Lightning in the app: if `STREAMHOUSE_ENVIRONMENT_ID`, `STREAMHOUSE_CLUSTER_ID`,
    `STREAMHOUSE_REGION`, `CONFLUENT_RTCE_API_KEY`, and `CONFLUENT_RTCE_API_SECRET` are all set,
    `_load_rtce` uses them (cloud `aws`) and reads no files.

- [ ] **Step 1: Write the failing tests:**

```python
def test_kafka_credentials_come_from_env_when_all_set(monkeypatch, tmp_path):
    for name in ENV_CREDENTIALS.values():
        monkeypatch.setenv(name, name.lower())
    assert extract_kafka_credentials("aws", tmp_path)["kafka_api_key"] == "streamhouse_kafka_api_key"

def test_partial_env_falls_back_to_state(monkeypatch, tmp_path):
    monkeypatch.setenv("STREAMHOUSE_KAFKA_API_KEY", "x")
    with pytest.raises(FileNotFoundError):
        extract_kafka_credentials("aws", tmp_path)

def test_app_reads_lightning_settings_from_env(monkeypatch):
    monkeypatch.setattr(airport_app.rtce, "_find_credentials_file", lambda: pytest.fail("read a file"))
    for name, value in {
        "STREAMHOUSE_ENVIRONMENT_ID": "env-1", "STREAMHOUSE_CLUSTER_ID": "lkc-1", "STREAMHOUSE_REGION": "us-east-1",
        "CONFLUENT_RTCE_API_KEY": "k", "CONFLUENT_RTCE_API_SECRET": "s",
    }.items():
        monkeypatch.setenv(name, value)
    assert Deployment().lightning_url == "https://sql.us-east-1.aws.confluent.cloud/query/v1alpha1"
```

- [ ] **Step 2: Run them to make sure they fail.** Run: `uv run pytest tests/test_app.py tests/test_continuous.py -v`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the suite.** Run: `uv run pytest -q`. Expected: all pass.
- [ ] **Step 5: Commit.** `git commit -am "Read Kafka and Lightning settings from environment variables"`

---

### Task 7: Password on the app

The app is reachable from the internet, so it gets a shared password when one is configured.

**Files:**
- Modify: `scripts/airport_app.py`
- Test: `tests/test_app.py`

**Interfaces:**
- Produces: a FastAPI HTTP middleware that, when the environment variable `APP_PASSWORD` is non-empty,
  requires HTTP Basic auth with any username and that password (compared with
  `secrets.compare_digest`) on every path. It answers `401` with `WWW-Authenticate: Basic realm="River Air"`
  otherwise. It reads `APP_PASSWORD` per request, so tests can set it with monkeypatch.

- [ ] **Step 1: Write the failing tests:**

```python
def test_no_password_means_no_login(monkeypatch):
    monkeypatch.delenv("APP_PASSWORD", raising=False)
    assert TestClient(airport_app.app).get("/").status_code == 200

def test_password_required_when_set(monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", "s3cret")
    client = TestClient(airport_app.app)
    assert client.get("/").status_code == 401
    assert client.get("/", auth=("demo", "wrong")).status_code == 401
    assert client.get("/", auth=("demo", "s3cret")).status_code == 200
    assert client.get("/static/app.js").status_code == 401
```

  (Use a real file under `scripts/static/` in place of `app.js` if the name differs.)

- [ ] **Step 2: Run them to make sure they fail.** Run: `uv run pytest tests/test_app.py -v`.
- [ ] **Step 3: Implement the middleware.**
- [ ] **Step 4: Run the suite.** Run: `uv run pytest -q`. Expected: all pass.
- [ ] **Step 5: Commit.** `git commit -am "Protect the app with an optional shared password"`

---

### Task 8: The agent handles every flight in continuous mode

**Files:**
- Modify: `terraform/airline-demo/variables.tf`, `terraform/airline-demo/main.tf`
  (`confluent_flink_statement.impacted_passengers`), `terraform/airline-demo/outputs.tf`
- Test: `tests/test_recovery_agent.py`

**Interfaces:**
- Produces:
  - **Variable:** `variable "continuous" { type = bool, default = false }`, described as "Always-on
    mode: the recovery agent handles every flight, and the continuous generator writes no live offers."
  - **Statement:** `impacted_passengers` uses
    `var.continuous ? replace(file(...27...), local.agent_flight_filter, "") : file(...27...)`, where
    `local.agent_flight_filter = "    AND SPLIT_INDEX(inbound_flight_id, '-', 0) IN ('RA417', 'RA183', 'RA185')\n"`.
  - **Output:** `output "continuous" { value = var.continuous }`.
  - **Tableflow:** a local `tableflow_topics = var.continuous ? distinct(concat(var.tableflow_topics,
    ["passenger_recommendations"])) : var.tableflow_topics`, used by `confluent_tableflow_topic.analytics`
    and the `tableflow_topics` output in place of `var.tableflow_topics`.
  - `sql/27` is unchanged, so `tests/test_workshop.py` still matches the guide.

- [ ] **Step 1: Write the failing test:**

```python
def test_continuous_mode_drops_exactly_the_agent_flight_filter():
    main = (ROOT / "terraform/airline-demo/main.tf").read_text()
    line = re.search(r'agent_flight_filter\s*=\s*"(.*)"', main).group(1).encode().decode("unicode_escape")
    assert line in _sql("27")  # the replace() can't silently match nothing
    assert "var.continuous ? replace(" in main

def test_continuous_mode_turns_on_tableflow_for_both_topics():
    tf = (ROOT / "terraform/airline-demo/analytics.tf").read_text() + (ROOT / "terraform/airline-demo/main.tf").read_text()
    assert '["passenger_recommendations"]' in tf and "local.tableflow_topics" in tf
```

- [ ] **Step 2: Run it to make sure it fails.** Run: `uv run pytest tests/test_recovery_agent.py -v`.
- [ ] **Step 3: Implement it, and update the `enable_recovery_agent` description** to mention
  continuous mode.
- [ ] **Step 4: Validate.**
  Run: `uv run pytest -q && terraform -chdir=terraform/airline-demo validate`. Expected: pass, and
  `Success! The configuration is valid.` (run `terraform init -backend=false` first if needed).
- [ ] **Step 5: Commit.** `git commit -am "Let the recovery agent handle every flight in always-on mode"`

---

### Task 9: Container image and the `terraform/always-on` root

**Files:**
- Create: `Dockerfile`, `.dockerignore`
- Create: `terraform/always-on/{providers.tf,variables.tf,main.tf,aws.tf,outputs.tf}`
- Test: `tests/test_always_on.py` (new)

**Interfaces:**
- **Image:**
  - Base `ghcr.io/astral-sh/uv:python3.12-bookworm-slim`.
  - Runs `uv sync --frozen --no-dev` with no credentials in any layer, and sets `ENTRYPOINT ["uv", "run", "--no-sync"]`.
  - App command: `airport-app --host 0.0.0.0 --port 8000`. Generator command: `airport-datagen --continuous`.
  - `.dockerignore` excludes `credentials.env`, `tmp/`, `terraform/`, `.venv/`, `.git/`, `docs/`,
    `tests/`, `*.tfstate*`.
- **Root inputs:**
  - Reads `../core/terraform.tfstate` the way `terraform/airline-demo/main.tf` does.
  - `variable "image_uri" { type = string }`: the ECR image with its tag, set by deploy.
  - `variable "rtce_api_key"` and `variable "rtce_api_secret"`, both sensitive.
  - The AWS provider uses the operator's ambient AWS credentials, in `local.core.cloud_region`.
- **Resources:** all named `${lower(local.core.resource_name)}-…`, for example
  `river-air-demo-flight-board`.
  - `aws_ecr_repository.images`, with `force_delete = true`.
  - `random_password.app` (24 characters, no special characters).
  - `aws_secretsmanager_secret.settings` and a secret version holding JSON with the six
    `STREAMHOUSE_*` Kafka/SR values (from core outputs), `STREAMHOUSE_ENVIRONMENT_ID`,
    `STREAMHOUSE_CLUSTER_ID`, `STREAMHOUSE_REGION`, `CONFLUENT_RTCE_API_KEY`,
    `CONFLUENT_RTCE_API_SECRET`, and `APP_PASSWORD`. Set `recovery_window_in_days = 0`.
  - **App:** `aws_apprunner_service.flight_board` with image `var.image_uri`, port 8000, and
    `runtime_environment_secrets` mapping each name to `"${secret_arn}:NAME::"`. It has an auto-scaling
    configuration with `min_size = 1`, `max_size = 1`, health check path `/static/index.html` or
    TCP, an ECR access role, and an instance role allowed `secretsmanager:GetSecretValue` on the secret.
  - **Generator:** `aws_ecs_cluster.demo` and `aws_ecs_task_definition.generator` (Fargate, 512 CPU,
    1024 MiB, the same `secrets`, logs to `aws_cloudwatch_log_group.generator` with 14-day retention).
    Then `aws_ecs_service.generator` with `desired_count = 1`, the default VPC's subnets, and
    `assign_public_ip = true`, plus a security group allowing all egress and no ingress.
- **Outputs:** `app_url` (`https://${service_url}`), `app_password` (sensitive),
  `ecr_repository_url`, `generator_log_group`.

- [ ] **Step 1: Write the failing static tests** in `tests/test_always_on.py`:

```python
ROOT = Path(__file__).parent.parent
TF = ROOT / "terraform/always-on"

def test_image_never_contains_credentials():
    ignored = (ROOT / ".dockerignore").read_text().split()
    assert {"credentials.env", "tmp/", "terraform/", ".git/"} <= set(ignored)

def test_no_email_addresses_in_tracked_terraform():
    text = "".join(p.read_text() for p in TF.glob("*.tf"))
    assert "@confluent.io" not in text

def test_generator_runs_continuous_and_app_runs_on_8000():
    aws = (TF / "aws.tf").read_text()
    assert '"airport-datagen", "--continuous"' in aws and "8000" in aws
```

- [ ] **Step 2: Run them to make sure they fail.** Run: `uv run pytest tests/test_always_on.py -v`.
- [ ] **Step 3: Write the Dockerfile, `.dockerignore`, and the Terraform root.**
- [ ] **Step 4: Verify.**
  Run: `uv run pytest -q`, then `terraform -chdir=terraform/always-on init -backend=false && terraform -chdir=terraform/always-on validate`,
  then `docker build --platform linux/amd64 -t streamhouse-demo:test .`.
  Expected: tests pass, `Success! The configuration is valid.`, and the image builds.
  Then run `docker run --rm streamhouse-demo:test airport-datagen --continuous --dry-run --now 2026-11-12T14:00:00Z --until 2026-11-12T14:01:00Z | head -3`
  and expect JSON rows.
- [ ] **Step 5: Commit.** `git add Dockerfile .dockerignore terraform/always-on tests/test_always_on.py && git commit -m "Add the always-on AWS services"`

---

### Task 10: `uv run deploy --always-on` and `uv run destroy` support

**Files:**
- Modify: `scripts/deploy.py`, `scripts/destroy.py`
- Test: `tests/test_always_on.py`

**Interfaces:**
- Consumes:
  - `run_continuous` / `continuous_history` (Task 5), the `continuous` variable (Task 8), and the
    always-on root and its outputs (Task 9).
  - The existing `_deploy`, `setup_rtce.main`, `_start_agent`, and `_save_env_safe`.
- Produces:
  - **The flag:** `--always-on` (mutually exclusive with `--workshop` and `--video-2`). It implies
    `--with-labs` and `--lab flight-disruptions`, and it saves `TF_VAR_always_on=true`,
    `TF_VAR_continuous=true`, and `TF_VAR_deployment_name=river-air-demo` (unless one is already
    saved) to `credentials.env`.
  - **Demo users:** the first run asks for the emails, comma-separated, and saves them as
    `DEMO_USER_EMAILS` in `credentials.env`. The users were already invited by hand (Task 0).
  - `_grant_demo_users(env_id: str, emails: list[str]) -> list[str]`. It looks each email up in
    `confluent iam user list -o json`, falling back to `confluent iam user invitation list -o json`'s
    `user` column for invitations not yet accepted. Then it runs
    `confluent iam rbac role-binding create --principal User:<id> --role EnvironmentAdmin --environment <env_id>`
    for each user who doesn't already have the binding (checked with
    `confluent iam rbac role-binding list --principal User:<id> --environment <env_id> -o json`), so a
    rerun is harmless. It returns the emails it couldn't find, and deploy prints them as a warning
    without failing.
  - **Saved mode:** later plain `uv run deploy` runs see `TF_VAR_always_on` and behave as `--always-on`.
  - **Data flags:** `--reset-data` (only with `--always-on`) tombstones the data first, through the
    existing `reset()` plus the continuous keys from `RETENTION_DAYS` days.
  - `_finish_always_on(root) -> None`, which runs in order:
    1. `run_continuous(SEED, history=True, until=now)` from the laptop, once. This publishes history and
       the current state and doesn't stream.
    2. `setup_rtce.main(["--client", "none"])`.
    3. `_start_agent(root)`.
    4. `confluent kafka topic update <topic> --config cleanup.policy=compact --cluster <id> --environment <id>`
       for `flight_status`, `passenger_itineraries`, and `hotel_inventory`.
    5. Build and push the image: `aws ecr get-login-password | docker login`, then
       `docker build --platform linux/amd64`, then `docker push`, tagged with `git rev-parse --short HEAD`.
       Before this, a bootstrap `terraform apply -target=aws_ecr_repository.images` creates the
       repository.
    6. `run_terraform(root / "terraform/always-on")` with `TF_VAR_image_uri`, `TF_VAR_rtce_api_key`, and
       `TF_VAR_rtce_api_secret` set.
    7. `_grant_demo_users(env_id, emails)`.
    8. Write `tmp/always-on-card.env` with `ALWAYS_ON_APP_URL`, `ALWAYS_ON_APP_PASSWORD`, and
       `ALWAYS_ON_DEMO_USERS`, and print it.
  - **Never starts the laptop stream:** the always-on flow never calls `_run_datagen`.
  - **Destroy:** `uv run destroy` destroys `always-on` before `airline-demo` and `core` when
    `terraform/always-on/terraform.tfstate` exists.
- **Preflight:** `_preflight_always_on(creds: dict) -> None` runs before anything is created. It stops
  with a clear message when:
  - `docker` or `aws` is missing, or `aws sts get-caller-identity` fails;
  - `TF_VAR_aws_tableflow_access_key` is empty (`--always-on` requires AWS credentials, so Tableflow is
    never skipped);
  - `aws iam simulate-principal-policy` for the caller denies any of `iam:CreateRole`,
    `iam:PutRolePolicy`, `iam:AttachRolePolicy`, `iam:CreatePolicy`, or `iam:PassRole`. If the
    simulation itself is denied, it prints a warning and continues.

- [ ] **Step 1: Write the failing tests.** Follow the monkeypatch style of
  `tests/test_recovery_agent.py::test_deploy_finishes_with_data_then_rtce_then_the_agent`:

```python
def test_always_on_finish_order(monkeypatch, tmp_path):
    calls = []
    # monkeypatch run_continuous, setup_rtce.main, _start_agent, _compact_sources, _push_image, run_terraform, _write_card
    deploy._finish_always_on(tmp_path)
    assert calls == ["history", "rtce", "agent", "compact", "image", "terraform", "users", "card"]

def test_grant_demo_users_binds_only_missing_roles(monkeypatch):
    # Stub subprocess.run: user list has a+1@x.io (u-1); invitation list has a+2@x.io (u-2);
    # role-binding list shows u-1 already EnvironmentAdmin.
    missing = deploy._grant_demo_users("env-1", ["a+1@x.io", "a+2@x.io", "a+9@x.io"])
    assert missing == ["a+9@x.io"]
    # exactly one role-binding create, for User:u-2 with --role EnvironmentAdmin --environment env-1

def test_always_on_never_starts_the_laptop_stream(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, "_run_datagen", lambda *a, **k: pytest.fail("laptop stream started"))
    # Same stubs as test_always_on_finish_order; _finish_always_on returns normally.

def test_preflight_stops_without_aws_credentials(monkeypatch):
    monkeypatch.setattr(deploy.shutil, "which", lambda name: f"/usr/bin/{name}")
    with pytest.raises(SystemExit, match="AWS credentials"):
        deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": ""})

def test_preflight_stops_when_iam_roles_are_denied(monkeypatch):
    # Stub subprocess.run: sts succeeds; simulate-principal-policy returns one "implicitDeny" for iam:CreateRole.
    with pytest.raises(SystemExit, match="iam:CreateRole"):
        deploy._preflight_always_on({"TF_VAR_aws_tableflow_access_key": "AKIA..."})

def test_always_on_rejects_workshop(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["deploy", "--always-on", "--workshop"])
    with pytest.raises(SystemExit):
        deploy.main()

def test_destroy_takes_down_always_on_first(monkeypatch, tmp_path):
    (tmp_path / "terraform/always-on").mkdir(parents=True)
    (tmp_path / "terraform/always-on/terraform.tfstate").write_text("{}")
    # Follow tests/test_destroy.py: record the roots destroy visits; assert the first is "always-on",
    # then airline-demo before core. Without the state file, always-on isn't visited.
```

- [ ] **Step 2: Run them to make sure they fail.** Run: `uv run pytest tests/test_always_on.py -v`.
- [ ] **Step 3: Implement.** Keep every new path behind `--always-on` or `TF_VAR_always_on`. The default
  deploy's code path must not change.
- [ ] **Step 4: Run the suite and lint.**
  Run: `uv run pytest -q && uv run ruff check scripts tests`. Expected: all pass.
- [ ] **Step 5: Commit.** `git commit -am "Deploy and destroy the always-on demo with one command"`

---

### Task 11: Docs and the live deployment

**Files:**
- Create: `docs/flight-disruptions/always-on.md`, the operator guide.
- Modify: `README.md` (one short section linking to the guide), `AGENTS.md`, and
  `docs/flight-disruptions/data-model.md` (the continuous mode section).

- [ ] **Step 1: Write the operator guide.** It covers:
  - **Prerequisites:** Docker, the AWS CLI logged in, and the Confluent login.
  - **Deploying:** `uv run deploy --always-on`, and what it creates (with a rough monthly cost line
    left for the operator to fill in).
  - **The card:** where the URL and password are.
  - **Rotating the password:** `terraform -chdir=terraform/always-on apply -replace=random_password.app`,
    then `aws apprunner start-deployment` and `aws ecs update-service --force-new-deployment`.
  - **Changing users:** invite with `confluent iam user invitation create <email>`, add the email to
    `DEMO_USER_EMAILS`, then rerun deploy. To remove a user, run
    `confluent iam user delete <user-id>`.
  - **Resetting the data:** `--reset-data`.
  - **Logs:** CloudWatch for the generator, App Runner logs for the app.
  - **Tearing down:** `uv run destroy`.
  - **Cost:** a line recording the measured monthly cost from Step 5b.
  - **A warning to pass on to the users:** pasting workshop SQL as written replaces the pipeline's
    tables. Use new table names.
- [ ] **Step 2: Update `AGENTS.md`.** Add one "Demo direction" bullet describing always-on mode,
  `--continuous`, `TF_VAR_continuous` dropping the `sql/27` flight filter, and `terraform/always-on/`,
  and add the new root to "Repository layout".
- [ ] **Step 3: Check the docs.** Run: `uv run pytest -q`. Expected: all pass, including
  `tests/test_workshop.py`.
- [ ] **Step 4: Commit.** `git commit -am "Document the always-on demo"`
- [ ] **Step 5: Deploy for real (operator, with the user's go-ahead).** Run `uv run deploy --always-on`,
  then check:
  - The app URL asks for the password and then shows flights.
  - Within 15 minutes, `passenger_recommendations` has agent rows for flights other than RA417, RA183,
    and RA185.
  - Within 2 hours, some agent offers turn BOOKED.
  - The ECS task survives `aws ecs update-service --force-new-deployment` and the board carries on
    where it was.
  - One demo user can sign in and open the environment.
  - Scene 3 works for the operator: `flight_impact` and `passenger_recommendations` show up as tables in
    the Glue database, and Athena can query them.
- [ ] **Step 5b: Cost checkpoint (operator, before anyone gets the link).** Leave it running for 2
  days. Then read the daily cost in the Confluent Cloud Console (Billing & payment → Cost) and in AWS
  Cost Explorer (filtered to the region), and record both as a monthly projection in
  `docs/flight-disruptions/always-on.md`. Share the card with colleagues only after the user has
  accepted that number.
- [ ] **Step 6: Push.** Run `git push-external -u origin continuous-demo-on-aws`, and open the PR only
  when the user asks.
