from datetime import datetime, timezone
from decimal import Decimal

from scripts.airport_datagen import GRAND_HYATT, OFFER_EXPIRY, OFFERS, rooms_at
from scripts.offer_customers import OfferWatcher, decide

AT = datetime(2026, 11, 12, 10)


def offer(passenger, n, flight="RA601-20261112", hotel=None, status="OFFERED", at=AT):
    return {
        "passenger_id": passenger,
        "recommended_flight_id": flight,
        "hotel_name": hotel,
        "status": status,
        "hotel_cost": Decimal("189.00") if hotel else None,
        "impacted_at": at,
        "recommended_at": at,
    }


def pair(pid):
    return {f"{pid}-O1": offer(pid, 1), f"{pid}-O2": offer(pid, 2)}


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
    assert rooms_at(42, late)[GRAND_HYATT] == 0
    switched = 0
    for i in range(1, 200):
        pid = f"P-1112-101-{i:03d}"
        offers = {
            f"{pid}-O1": offer(pid, 1, hotel=GRAND_HYATT, at=late),
            f"{pid}-O2": offer(pid, 2, at=late),
        }
        for at, key, value in decide(42, offers):
            if value["status"] == "BOOKED" and at.date() == late.date():
                assert key.endswith("-O2")
                switched += 1
    assert switched > 0


def test_rows_keep_every_field_but_status():
    pid = "P-1112-101-001"
    offers = pair(pid)
    for _, key, value in decide(42, offers):
        assert value == {**offers[key], "status": value["status"]}
    assert {k for _, k, _ in decide(42, offers)} == set(offers)


def test_no_flights_means_both_expire():
    pid = "P-1112-101-001"
    offers = pair(pid)
    for value in offers.values():
        value["recommended_flight_id"] = None
    assert all(at == AT + OFFER_EXPIRY and v["status"] == "CLOSED" for at, _, v in decide(42, offers))


def test_both_sold_out_expire():
    late = datetime(2026, 11, 12, 18, 30)
    assert rooms_at(42, late)[GRAND_HYATT] == 0
    expired = 0
    for i in range(1, 100):
        pid = f"P-1112-101-{i:03d}"
        offers = {f"{pid}-O{n}": offer(pid, n, hotel=GRAND_HYATT, at=late) for n in (1, 2)}
        for at, _, value in decide(42, offers):
            assert value["status"] == "CLOSED" and at == late + OFFER_EXPIRY
            expired += 1
    assert expired > 0


class FakeMessage:
    count = 0

    def __init__(self, key, value, partition=0):
        self._key, self._value, self._partition = key, value, partition
        self._offset = FakeMessage.count
        FakeMessage.count += 1

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None

    def key(self):
        return self._key.encode()

    def value(self):
        return self._value

    def topic(self):
        return OFFERS


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None


def watcher(messages, ends=None):
    # The fake "bytes" are the offer dicts themselves, and the deserializer passes them through.
    return OfferWatcher.for_tests(42, FakeConsumer(messages), lambda value, ctx: value, ends)


def test_watcher_decides_once_when_both_offers_arrive():
    pid = "P-1112-101-001"
    w = watcher([FakeMessage(f"{pid}-O1", offer(pid, 1))])
    assert w.poll(0) == []
    w.consumer.messages += [FakeMessage(f"{pid}-O2", offer(pid, 2))]
    rows = w.poll(0)
    assert len(rows) == 2 and {k for _, k, _ in rows} == {f"{pid}-O1", f"{pid}-O2"}
    w.consumer.messages += [FakeMessage(f"{pid}-O1", offer(pid, 1))]
    assert w.poll(0) == []


def test_watcher_normalizes_timestamps_and_honors_tombstones():
    pid = "P-1112-101-002"
    aware = datetime(2026, 11, 12, 10, tzinfo=timezone.utc)
    first = offer(pid, 1, at=aware)
    w = watcher([FakeMessage(f"{pid}-O1", first), FakeMessage(f"{pid}-O2", None)])
    assert w.poll(0) == []
    w.consumer.messages += [FakeMessage(f"{pid}-O2", offer(pid, 2, at=aware))]
    rows = w.poll(0)
    assert rows and all(v["recommended_at"].tzinfo is None for _, _, v in rows)
    assert all(v["impacted_at"] == AT for _, _, v in rows)


def test_still_open_tracks_the_current_view():
    pid = "P-1112-101-003"
    w = watcher([FakeMessage(f"{pid}-O1", offer(pid, 1))])
    w.poll(0)
    assert not w.still_open(pid)
    w.consumer.messages += [FakeMessage(f"{pid}-O2", offer(pid, 2))]
    w.poll(0)
    assert w.still_open(pid)
    w.consumer.messages += [FakeMessage(f"{pid}-O1", offer(pid, 1, status="BOOKED"))]
    w.poll(0)
    assert not w.still_open(pid)
    assert not w.still_open("nobody")


def test_replay_does_not_decide_on_a_stale_view():
    pid = "P-1112-101-004"
    start = FakeMessage.count
    messages = [
        FakeMessage(f"{pid}-O1", offer(pid, 1)),
        FakeMessage(f"{pid}-O2", offer(pid, 2)),
        FakeMessage(f"{pid}-O1", offer(pid, 1, status="BOOKED")),
        FakeMessage(f"{pid}-O2", offer(pid, 2, status="CLOSED")),
    ]
    w = watcher(messages[:2], ends={0: start + 4})
    assert w.poll(0) == []  # still replaying: the settled rows are ahead
    w.consumer.messages += messages[2:]
    assert w.poll(0) == []  # caught up, and the passenger is settled
    fresh = "P-1112-101-005"
    w.consumer.messages += [FakeMessage(f"{fresh}-O1", offer(fresh, 1)), FakeMessage(f"{fresh}-O2", offer(fresh, 2))]
    assert len(w.poll(0)) == 2
