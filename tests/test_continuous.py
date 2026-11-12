from datetime import date, datetime, timedelta
from itertools import pairwise

import pytest

from scripts import airport_datagen as g

DAY = date(2026, 11, 12)
MIDNIGHT = datetime(2026, 11, 12)
NOW = datetime(2026, 11, 12, 14, 7, 30)
STAGE = {"ON_TIME": 0, "DELAYED": 0, "BOARDING": 1, "DEPARTED": 2, "LANDED": 2}


def around_the_clock_day():
    return g.build_day(g.SEED, DAY, MIDNIGHT, live=True, schedule=g.AROUND_THE_CLOCK)


def test_around_the_clock_has_180_flights_spread_over_24_hours():
    day = around_the_clock_day()
    assert len(day.arrivals) == 90 and len(day.departures) == 90
    hours = {f.scheduled.hour for f in day.flights}
    assert hours == set(range(24))
    assert all(MIDNIGHT <= f.scheduled < MIDNIGHT + timedelta(days=1) for f in day.flights)


def test_each_destination_has_one_overnight_gap_staggered_across_the_day():
    day = around_the_clock_day()
    starts = []
    for spoke in g.SPOKES:
        times = sorted(f.scheduled for f in day.departures if f.destination == spoke)
        gaps = [(b - a, a) for a, b in zip(times, [*times[1:], times[0] + timedelta(days=1)], strict=True)]
        long = [a for gap, a in gaps if gap > g.OVERNIGHT]
        assert len(long) == 1
        starts.append(long[0].hour)
    assert len(set(starts)) >= 8  # staggered, not all at the same hour


def test_continuous_days_have_eight_ra417s_from_chicago_every_three_hours():
    day = around_the_clock_day()
    heroes = day.heroes
    assert [f.number for f in heroes] == [g.HERO_NUMBER] * 8
    assert all(f.origin == g.HERO_ORIGIN and f.destination == g.HUB for f in heroes)
    assert [f.scheduled for f in heroes] == [MIDNIGHT + timedelta(minutes=m) for m in g.CONTINUOUS_HERO_MINUTES]
    assert heroes[0].key == "RA417-20261112-0040"
    assert len({f.key for f in day.flights}) == 180
    assert day.agent_flights == []


def test_scripted_schedule_is_unchanged():
    day = g.build_day(g.SEED, DAY, MIDNIGHT, live=True)
    assert day.schedule is g.SCRIPTED and day.hero.number == g.HERO_NUMBER
    assert day.heroes == [day.hero]


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


def test_ra417_passenger_keys_name_the_flight_time_and_are_unique():
    day = g.continuous_day(g.SEED, DAY)
    first = day.heroes[0]
    riders = [p for p in day.passengers if p.inbound is first]
    assert riders and all(p.key.startswith("P-1112-417-0040-") for p in riders)
    keys = [p.key for p in day.passengers]
    assert len(keys) == len(set(keys))


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


def test_grand_hyatt_sells_out_each_evening_and_refills_at_midnight():
    assert g.rooms_at(g.SEED, MIDNIGHT + timedelta(hours=19))[g.GRAND_HYATT] == 0
    assert g.rooms_at(g.SEED, MIDNIGHT + timedelta(minutes=1))[g.GRAND_HYATT] > 0
    assert all(r >= 1 for h, r in g.rooms_at(g.SEED, MIDNIGHT + timedelta(hours=19)).items() if h != g.GRAND_HYATT)


def test_hotel_rooms_publish_every_15_seconds():
    rows = g.continuous_hotel_rooms(g.SEED, DAY)
    assert len(rows) == 3 * (24 * 3600 // 15)
    assert [r[0] for r in rows] == sorted(r[0] for r in rows)


def test_hotel_rooms_stay_in_bounds_all_day_and_hyatt_is_zero_from_sellout_to_midnight():
    sellout = MIDNIGHT + timedelta(hours=g.CONTINUOUS_SELLOUT_HOUR)
    for at, hotel, rooms in g.continuous_hotel_rooms(g.SEED, DAY):
        if hotel == g.GRAND_HYATT:
            assert 0 <= rooms <= g.GRAND_HYATT_ROOMS
            assert (rooms == 0) == (at >= sellout)
        else:
            assert rooms >= 1


def test_rooms_at_midnight_is_the_new_days_full_counts_and_is_cached():
    full = {g.GRAND_HYATT: g.GRAND_HYATT_ROOMS, g.MARRIOTT: g.MARRIOTT_ROOMS, g.HILTON: g.HILTON_ROOMS}
    assert g.rooms_at(g.SEED, MIDNIGHT) == full
    assert g.rooms_at(g.SEED, MIDNIGHT - timedelta(seconds=1))[g.GRAND_HYATT] == 0
    assert g.continuous_hotel_rooms(g.SEED, DAY) is g.continuous_hotel_rooms(g.SEED, DAY)


def test_restart_republishes_the_same_state():
    assert g.continuous_state(g.SEED, NOW) == g.continuous_state(g.SEED, NOW)
    before = {k: v for t, k, v in g.continuous_state(g.SEED, NOW) if t == g.FLIGHTS}
    after = {k: v for t, k, v in g.continuous_state(g.SEED, NOW + timedelta(minutes=10)) if t == g.FLIGHTS}
    assert before.keys() == after.keys() and len(before) == 3 * 180
    for key, row in before.items():
        assert STAGE[after[key]["status"]] >= STAGE[row["status"]]
        # A delay never goes back to on time, and a severe one may shrink by at most 3 minutes. (On-time flights
        # aren't checked: an early-arrival notice plus a minute's revision can move them by a few minutes.)
        if row["status"] == "DELAYED":
            assert after[key]["status"] != "ON_TIME"
            assert after[key]["estimated_time"] >= row["estimated_time"] - timedelta(minutes=3)


def test_state_has_current_hotels_and_every_live_days_itineraries():
    state = g.continuous_state(g.SEED, NOW)
    hotels = {k: v["available_rooms"] for t, k, v in state if t == g.HOTELS}
    assert hotels == g.rooms_at(g.SEED, NOW)
    itineraries = {k for t, k, _ in state if t == g.ITINERARIES}
    days = [g.continuous_day(g.SEED, DAY + timedelta(days=n)) for n in (-1, 0, 1)]
    assert itineraries == {p.key for d in days for p in d.passengers}


def test_events_cross_midnight_without_gaps():
    start, end = datetime(2026, 11, 12, 23, 50), datetime(2026, 11, 13, 0, 10)
    events = g.continuous_events(g.SEED, start, end)
    assert events == sorted(events, key=lambda event: event[0])
    assert all(start < at <= end for at, *_ in events)
    ticks = {}
    for at, topic, key, value in events:
        # Streamed ticks only: day D+2's rows published at midnight and old-day tombstones aren't ticks.
        if topic == g.FLIGHTS and value is not None and key.split("-")[1] != "20261114":
            ticks.setdefault(key, []).append(at)
    assert any("-20261112" in k for k in ticks) and any("-20261113" in k for k in ticks)
    assert all(b - a == g.UPDATE_EVERY for times in ticks.values() for a, b in pairwise(times))
    assert any("-20261114" in k for _, t, k, _ in events if t == g.FLIGHTS)  # D+1 published at midnight
    assert any(t == g.ITINERARIES and k.startswith("P-1114-") for _, t, k, _ in events)
    assert len({(at, k) for at, _, k, _ in events}) == len(events)


def test_hotels_refill_at_midnight_once():
    midnight = datetime(2026, 11, 13)
    events = g.continuous_events(g.SEED, midnight - timedelta(seconds=30), midnight + timedelta(seconds=30))
    hotels = [(at, k, v["available_rooms"]) for at, t, k, v in events if t == g.HOTELS]
    assert len(hotels) == 3 * 4
    at_midnight = {k: rooms for at, k, rooms in hotels if at == midnight}
    assert at_midnight == g.rooms_at(g.SEED, midnight) and at_midnight[g.GRAND_HYATT] == g.GRAND_HYATT_ROOMS


def test_midnight_tombstones_the_day_past_retention():
    events = g.continuous_events(g.SEED, datetime(2026, 11, 12, 23, 59), datetime(2026, 11, 13, 0, 1))
    old = (date(2026, 11, 13) - timedelta(days=g.RETENTION_DAYS)).strftime("%Y%m%d")
    tombstones = {(t, k) for _, t, k, v in events if v is None}
    assert sum(1 for t, k in tombstones if t == g.FLIGHTS and k.split("-")[1] == old) == 180
    assert any(t == g.ITINERARIES for t, _ in tombstones)
    assert any(t == g.OFFERS and k.endswith("-O1") for t, k in tombstones)
    assert any(t == g.OFFERS and k.endswith("-O2") for t, k in tombstones)


def test_continuous_writes_no_live_offers():
    events = g.continuous_events(g.SEED, NOW, NOW + timedelta(hours=1))
    assert events and not any(t == g.OFFERS and v is not None for _, t, _, v in events)


def test_history_is_finished_and_has_completed_offers():
    rows = g.continuous_history(g.SEED, NOW)
    offers = [v for t, _, v in rows if t == g.OFFERS]
    assert offers and {v["status"] for v in offers} == {"BOOKED", "CLOSED"}
    flights = [(k, v) for t, k, v in rows if t == g.FLIGHTS]
    assert len(flights) == 29 * 180
    assert all(v["estimated_time"] < NOW and v["status"] in ("LANDED", "DEPARTED") for _, v in flights)
    assert all(v["connecting_flight_id"] for t, _, v in rows if t == g.ITINERARIES)
    dates = {k.split("-")[1] for k, _ in flights}
    assert min(dates) == "20261013" and max(dates) == "20261110"


def test_continuous_dry_run_prints_json(capsys):
    g.main(["--continuous", "--dry-run", "--now", "2026-11-12T14:00:00Z", "--until", "2026-11-12T14:02:00Z"])
    out = capsys.readouterr().out
    assert '"topic": "flight_status"' in out and '"topic": "passenger_recommendations"' not in out


@pytest.mark.parametrize(
    "flags",
    [["--minutes", "5"], ["--speed", "2"], ["--resume"], ["--stream-only"], ["--offers", "agent"], ["--reset"]],
)
def test_continuous_rejects_scripted_flags(flags):
    with pytest.raises(SystemExit):
        g.main(["--continuous", *flags])


def test_history_needs_continuous():
    with pytest.raises(SystemExit):
        g.main(["--history", "--dry-run"])


class FakeWatcher:
    def __init__(self, rows, open_once):
        self.rows, self.open_once, self.checked = rows, open_once, []

    def poll(self, timeout, limit=500):
        rows, self.rows = self.rows, []
        return rows

    def still_open(self, passenger_id):
        self.checked.append(passenger_id)
        return self.open_once.pop(passenger_id, False)  # open the first time only, as after our own booking


def test_customer_rows_drop_when_someone_booked_in_the_app():
    def offer(passenger, n, status):
        return (NOW + timedelta(seconds=5), f"{passenger}-O{n}", {"passenger_id": passenger, "status": status})

    rows = [offer("P-A", 1, "BOOKED"), offer("P-A", 2, "CLOSED"), offer("P-B", 1, "CLOSED"), offer("P-B", 2, "BOOKED")]
    watcher = FakeWatcher(rows, {"P-A": False, "P-B": True})
    published = []
    g._stream_continuous(g.SEED, NOW, NOW + timedelta(minutes=1), lambda *row: published.append(row), watcher=watcher)
    offers = [(k, v["status"]) for t, k, v in published if t == g.OFFERS]
    assert offers == [("P-B-O1", "CLOSED"), ("P-B-O2", "BOOKED")]
    assert sorted(set(watcher.checked)) == ["P-A", "P-B"]
    assert any(t == g.FLIGHTS for t, _, _ in published)
