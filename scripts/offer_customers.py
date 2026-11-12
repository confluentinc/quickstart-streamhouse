"""Simulated customers: they decide on the offers the recovery agent writes in the always-on demo.

`decide` is pure and seeded per passenger. `OfferWatcher` feeds it from the offers topic.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .airport_datagen import BOOKING_SHARE, DECISION_SECONDS, OFFER_EXPIRY, OFFERS, rooms_at

Row = tuple[datetime, str, dict]


def decide(seed: int, offers: dict[str, dict]) -> list[Row]:
    """The customer's answer to a passenger's two offers, as timed rows to publish.

    Returns [] unless both offers are still OFFERED (a human got there first).
    """
    if len(offers) != 2 or any(v["status"] != "OFFERED" for v in offers.values()):
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
        for index in (first, 1 - first):
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
        consumer.subscribe([OFFERS])
        self._setup(seed, consumer, AvroDeserializer(registry))

    @classmethod
    def for_tests(cls, seed: int, consumer, deserializer) -> "OfferWatcher":
        watcher = cls.__new__(cls)
        watcher._setup(seed, consumer, deserializer)
        return watcher

    def _setup(self, seed, consumer, deserializer) -> None:
        self.seed = seed
        self.consumer = consumer
        self.deserializer = deserializer
        self.view: dict[str, dict[str, dict]] = {}
        self.decided: set[str] = set()

    def poll(self, timeout: float, limit: int = 500) -> list[Row]:
        touched = []
        for _ in range(limit):
            message = self.consumer.poll(timeout if not touched else 0)
            if message is None:
                break
            if message.error():
                continue
            key = message.key().decode()
            passenger = key.rsplit("-O", 1)[0]
            if message.value() is None:
                self.view.get(passenger, {}).pop(key, None)
            else:
                value = self.deserializer(message.value(), SerializationContext(message.topic(), MessageField.VALUE))
                self.view.setdefault(passenger, {})[key] = _naive_utc(value)
            touched.append(passenger)
        rows = []
        for passenger in dict.fromkeys(touched):
            if passenger in self.decided:
                continue
            answer = decide(self.seed, self.view.get(passenger, {}))
            if answer:
                self.decided.add(passenger)
                rows.extend(answer)
        return rows
