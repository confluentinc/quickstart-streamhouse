"""Simulated customers: they decide on the offers the recovery agent writes in the always-on demo.

`decide` is pure and seeded per passenger. `OfferWatcher` feeds it from the offers topic.
"""

import logging
import random
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .airport_datagen import BOOKING_SHARE, DECISION_SECONDS, OFFER_EXPIRY, OFFERS, _wall_clock, rooms_at

Row = tuple[datetime, str, dict]


LONE_OFFER_GRACE = timedelta(minutes=2)  # how long the watcher waits for a second offer before deciding on one


def decide(seed: int, offers: dict[str, dict]) -> list[Row]:
    """The customer's answer to a passenger's offers (two, or one when the agent found only one), as timed rows.

    Returns [] unless every offer is still OFFERED (a human got there first).
    """
    if len(offers) not in (1, 2) or any(v["status"] != "OFFERED" for v in offers.values()):
        return []
    keys = sorted(offers)
    passenger = offers[keys[0]]["passenger_id"]
    recommended_at = min(v["recommended_at"] for v in offers.values())
    rng = random.Random(f"{seed}:{passenger}:customer")
    books = rng.random() < BOOKING_SHARE
    decided_at = recommended_at + timedelta(seconds=rng.randint(*DECISION_SECONDS))
    first = 0 if rng.random() < 0.7 else 1
    chosen = None
    if books:
        rooms = None
        for index in (first, 1 - first) if len(keys) == 2 else (0,):
            offer = offers[keys[index]]
            if offer["recommended_flight_id"] is None:
                continue
            if offer["hotel_name"] is not None:
                rooms = rooms if rooms is not None else rooms_at(seed, decided_at)
                if rooms.get(offer["hotel_name"], 0) <= 0:
                    continue
            chosen = keys[index]
            break
    if chosen is None:
        expires = recommended_at + OFFER_EXPIRY
        return [(expires, key, {**offers[key], "status": "CLOSED"}) for key in keys]
    return [(decided_at, key, {**offers[key], "status": "BOOKED" if key == chosen else "CLOSED"}) for key in keys]


def _naive_utc(value: dict) -> dict:
    return {
        name: field.astimezone(timezone.utc).replace(tzinfo=None)
        if isinstance(field, datetime) and field.tzinfo
        else field
        for name, field in value.items()
    }


class OfferWatcher:
    """Reads the offers topic and answers, once per passenger, when both of their offers are open."""

    def __init__(self, credentials: dict, seed: int):
        registry = SchemaRegistryClient(
            {
                "url": credentials["schema_registry_url"],
                "basic.auth.user.info": (
                    f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}"
                ),
            }
        )
        consumer = Consumer(
            {
                "bootstrap.servers": credentials["bootstrap_servers"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": credentials["kafka_api_key"],
                "sasl.password": credentials["kafka_api_secret"],
                "group.id": f"airport-offer-customers-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        # Read every partition from the start, and note where each ends now: until the watcher gets there it is
        # replaying history, and a passenger's settled rows may still be ahead of their OFFERED ones.
        partitions = [
            TopicPartition(OFFERS, p) for p in consumer.list_topics(OFFERS, timeout=30).topics[OFFERS].partitions
        ]
        ends = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(partition, timeout=30)
            if high > low:
                ends[partition.partition] = high
        consumer.assign(partitions)
        self._setup(seed, consumer, AvroDeserializer(registry), ends)

    @classmethod
    def for_tests(
        cls, seed: int, consumer, deserializer, ends: dict[int, int] | None = None, now=None
    ) -> "OfferWatcher":
        """A watcher on a fake consumer. Without `now`, its clock stays at datetime.min, so a lone offer never
        waits out LONE_OFFER_GRACE and the tests decide only on pairs."""
        watcher = cls.__new__(cls)
        watcher._setup(seed, consumer, deserializer, ends or {}, now or (lambda: datetime.min))
        return watcher

    def _setup(self, seed, consumer, deserializer, ends: dict[int, int], now=None) -> None:
        self.ends = ends  # partition -> offset the topic ended at when we started
        self.caught = False
        self.pending: set[str] = set()
        self.lone: set[str] = set()  # passengers with one open offer, waiting out LONE_OFFER_GRACE for a second
        self.seed = seed
        self.consumer = consumer
        self.deserializer = deserializer
        self.now = now or _wall_clock  # naive UTC, like the offers' timestamps
        self.view: dict[str, dict[str, dict]] = {}
        self.decided: dict[str, dict[str, dict]] = {}  # passenger -> the offers as decide saw them

    def caught_up(self) -> bool:
        """True once the consumer's position has passed every startup end offset (and stays true).

        Position, not the last message offset: the topic can end in transaction markers or compaction gaps
        that are never delivered as messages.
        """
        if not self.caught:
            partitions = [TopicPartition(OFFERS, p) for p in self.ends]
            positions = {tp.partition: tp.offset for tp in self.consumer.position(partitions)} if partitions else {}
            self.caught = all(positions.get(p, 0) >= end for p, end in self.ends.items())
        return self.caught

    def still_open(self, passenger_id: str) -> bool:
        """True while the passenger's offers are all OFFERED and are the ones the customer answers.

        Before a decision that means both offers; after one, every offer decide saw must be unchanged (the app's
        sold-out path can rewrite one with a new hotel and leave it OFFERED), which also covers a lone offer.
        """
        offers = self.view.get(passenger_id, {})
        if not offers or any(v["status"] != "OFFERED" for v in offers.values()):
            return False
        seen = self.decided.get(passenger_id)
        if seen is None:
            return len(offers) == 2
        return all(offers.get(key) == value for key, value in seen.items())

    def _read(self, message) -> tuple[str, dict | None] | None:
        """(key, value) of one offers record, or None, with a warning, for one that can't be read."""
        where = f"{message.topic()} partition {message.partition()} offset {message.offset()}"
        if message.error():
            logging.warning("Skipping %s: %s", where, message.error())
            return None
        if message.key() is None:
            logging.warning("Skipping %s: it has no key", where)
            return None
        try:
            key = message.key().decode()
            if message.value() is None:
                return key, None
            value = self.deserializer(message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            return key, _naive_utc(value)
        except Exception as error:  # a bad record must not stop the generator, or it replays into it forever
            logging.warning("Skipping %s: %s", where, error)
            return None

    def _ready(self, passenger: str) -> bool:
        """Two open offers, or one that has waited LONE_OFFER_GRACE for a second."""
        offers = self.view.get(passenger, {})
        if len(offers) != 1:
            return True
        (offer,) = offers.values()
        if offer["status"] != "OFFERED":
            return True
        return self.now() - offer["recommended_at"] >= LONE_OFFER_GRACE

    def poll(self, timeout: float, limit: int = 500) -> list[Row]:
        touched = []
        for _ in range(limit):
            message = self.consumer.poll(timeout if not touched else 0)
            if message is None:
                break
            read = self._read(message)
            if read is None:
                continue
            key, value = read
            passenger = key.rsplit("-O", 1)[0]
            if value is None:
                self.view.get(passenger, {}).pop(key, None)
            else:
                self.view.setdefault(passenger, {})[key] = value
            touched.append(passenger)
        self.pending.update(touched)
        if not self.caught_up():
            return []
        rows = []
        pending, self.pending, self.lone = self.pending | self.lone, set(), set()
        for passenger in sorted(pending):
            if passenger in self.decided:
                continue
            if not self._ready(passenger):
                self.lone.add(passenger)  # checked again on later polls, even with no new message for them
                continue
            offers = self.view.get(passenger, {})
            answer = decide(self.seed, offers)
            if answer:
                self.decided[passenger] = {key: dict(value) for key, value in offers.items()}
                rows.extend(answer)
        return rows
