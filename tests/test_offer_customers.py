from datetime import datetime, timedelta, timezone
from decimal import Decimal

from confluent_kafka import TopicPartition

from scripts.airport_datagen import GRAND_HYATT, OFFER_EXPIRY, OFFERS, rooms_at
from scripts.offer_customers import LONE_OFFER_GRACE, OfferWatcher, decide

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
    def __init__(self, messages, trailing=0):
        self.messages = list(messages)
        self.next_offset = 0
        self.trailing = trailing  # offsets after the last message that are never delivered (markers, gaps)

    def poll(self, timeout):
        if not self.messages:
            return None
        message = self.messages.pop(0)
        self.next_offset = message.offset() + 1
        return message

    def position(self, partitions):
        offset = self.next_offset + (0 if self.messages else self.trailing)
        return [TopicPartition(p.topic, p.partition, offset) for p in partitions]


def watcher(messages, ends=None, trailing=0):
    # The fake "bytes" are the offer dicts themselves, and the deserializer passes them through.
    return OfferWatcher.for_tests(42, FakeConsumer(messages, trailing), lambda value, ctx: value, ends)


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


def test_catches_up_past_trailing_transaction_markers():
    pid = "P-1112-101-006"
    start = FakeMessage.count
    messages = [FakeMessage(f"{pid}-O1", offer(pid, 1)), FakeMessage(f"{pid}-O2", offer(pid, 2))]
    # Two undelivered markers follow the last message, so the topic ends at start + 4.
    w = watcher(messages, ends={0: start + 4}, trailing=2)
    assert len(w.poll(0)) == 2


class BrokenMessage(FakeMessage):
    """A message with a missing key, an undecodable key, or a broker error."""

    def __init__(self, key=None, value=b"x", error=None):
        super().__init__("", value)
        self._raw_key, self._error = key, error

    def key(self):
        return self._raw_key

    def error(self):
        return self._error


def test_watcher_skips_records_it_cannot_read(caplog):
    pid = "P-1112-101-010"

    def deserializer(value, ctx):
        if value == b"not avro":
            raise ValueError("Unknown magic byte")
        return value

    messages = [
        BrokenMessage(key=None),
        BrokenMessage(key=b"\xff\xfe"),
        BrokenMessage(key=f"{pid}-O9".encode(), value=b"not avro"),
        BrokenMessage(key=b"k", error="broker error"),
        FakeMessage(f"{pid}-O1", offer(pid, 1)),
        FakeMessage(f"{pid}-O2", offer(pid, 2)),
    ]
    w = OfferWatcher.for_tests(42, FakeConsumer(messages), deserializer)
    with caplog.at_level("WARNING"):
        rows = w.poll(0)
    assert {k for _, k, _ in rows} == {f"{pid}-O1", f"{pid}-O2"}
    assert caplog.text.count("Skipping") == 4
    assert "Unknown magic byte" in caplog.text and "not avro" not in caplog.text


def clocked_watcher(messages, clock):
    return OfferWatcher.for_tests(42, FakeConsumer(messages), lambda value, ctx: value, now=lambda: clock["now"])


def test_lone_offer_is_decided_after_the_grace_period():
    pid = "P-1112-101-011"
    clock = {"now": AT + timedelta(minutes=1)}
    w = clocked_watcher([FakeMessage(f"{pid}-O1", offer(pid, 1))], clock)
    assert w.poll(0) == []
    clock["now"] = AT + LONE_OFFER_GRACE
    rows = w.poll(0)  # no new message: the pending lone offer is checked again
    assert [k for _, k, _ in rows] == [f"{pid}-O1"]
    assert w.still_open(pid)
    assert w.poll(0) == []  # decided once


def test_lone_offer_is_not_decided_before_the_grace_period():
    pid = "P-1112-101-012"
    clock = {"now": AT + LONE_OFFER_GRACE - timedelta(seconds=1)}
    w = clocked_watcher([FakeMessage(f"{pid}-O1", offer(pid, 1))], clock)
    assert w.poll(0) == [] and w.poll(0) == []
    assert not w.still_open(pid)


def test_second_offer_within_the_grace_period_decides_the_pair():
    pid = "P-1112-101-013"
    clock = {"now": AT + timedelta(seconds=30)}
    w = clocked_watcher([FakeMessage(f"{pid}-O1", offer(pid, 1))], clock)
    assert w.poll(0) == []
    w.consumer.messages += [FakeMessage(f"{pid}-O2", offer(pid, 2))]
    clock["now"] = AT + timedelta(minutes=5)
    assert {k for _, k, _ in w.poll(0)} == {f"{pid}-O1", f"{pid}-O2"}
    assert w.poll(0) == []


def test_lone_offer_books_about_85_percent_and_the_rest_expire():
    results = [decide(42, {f"P-1112-101-{i:03d}-O1": offer(f"P-1112-101-{i:03d}", 1)}) for i in range(1, 401)]
    assert all(len(rows) == 1 for rows in results)
    booked = sum(rows[0][2]["status"] == "BOOKED" for rows in results)
    assert 300 <= booked <= 380
    assert all(at == AT + OFFER_EXPIRY for rows in results for at, _, v in rows if v["status"] == "CLOSED")


def test_lone_offer_with_a_sold_out_hotel_expires():
    late = datetime(2026, 11, 12, 18, 30)
    for i in range(1, 50):
        pid = f"P-1112-101-{i:03d}"
        [(at, _, value)] = decide(42, {f"{pid}-O1": offer(pid, 1, hotel=GRAND_HYATT, at=late)})
        assert value["status"] == "CLOSED" and at == late + OFFER_EXPIRY


def test_a_rewritten_offer_drops_the_simulated_answer():
    # The app's sold-out path rewrites an offer with a new hotel and leaves it OFFERED: the answer is stale.
    pid = "P-1112-101-014"
    w = watcher([FakeMessage(f"{pid}-O1", offer(pid, 1, hotel=GRAND_HYATT)), FakeMessage(f"{pid}-O2", offer(pid, 2))])
    assert len(w.poll(0)) == 2 and w.still_open(pid)
    w.consumer.messages += [FakeMessage(f"{pid}-O1", offer(pid, 1, hotel="Hilton San Francisco Airport Bayfront"))]
    w.poll(0)
    assert not w.still_open(pid)
