from datetime import date, datetime, timedelta

from scripts import airport_datagen as g

DAY = date(2026, 11, 12)
MIDNIGHT = datetime(2026, 11, 12)


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
