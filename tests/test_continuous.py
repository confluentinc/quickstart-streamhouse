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
        gaps = [(b - a, a) for a, b in zip(times, [*times[1:], times[0] + timedelta(days=1)], strict=True)]
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
    assert heroes[0].key == "RA417-20261112-0040"
    assert len({f.key for f in day.flights}) == 180
    assert day.agent_flights == []


def test_scripted_schedule_is_unchanged():
    day = g.build_day(g.SEED, DAY, MIDNIGHT, live=True)
    assert day.schedule is g.SCRIPTED and day.hero.number == g.HERO_NUMBER
    assert day.heroes == [day.hero]
