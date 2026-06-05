"""Seed Tom Nook's Kafka cluster with synthetic Animal Crossing data.

Creates the topics declared in ``topics.py`` if they do not yet exist, then
launches one asyncio task per topic that produces JSON records at the
configured rate. Designed to run continuously inside docker compose.

Notes on resilience:

* We use ``producer.send`` (not ``send_and_wait``) so each loop iteration
  can keep generating records at its configured cadence even while the
  underlying producer is retrying a partition whose leader is still being
  elected. Acks are still received in the background; failures bubble up
  via ``flush`` and per-topic exception logging.
* Each topic gets its own ``produce_loop`` wrapped in a broad exception
  handler so a hiccup on one topic does not cancel the others.
* A heartbeat task prints per-topic counters every 30 s so it is obvious
  which topics are actually flowing.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from topics import TOPICS, TopicSpec

_KNOWLEDGE = Path(__file__).parent / "knowledge"


def _load_names(filename: str) -> list[str]:
    with (_KNOWLEDGE / filename).open() as f:
        return [item["name"] for item in json.load(f)]


# Set to a fixed datetime to pin all generated timestamps to a specific
# reference point, e.g. datetime(2026, 10, 15, tzinfo=timezone.utc).
# Leave as None to use the current UTC time.
ANCHOR_DATE: datetime | None = None


def now_iso() -> str:
    return (ANCHOR_DATE if ANCHOR_DATE is not None else datetime.now(timezone.utc)).isoformat()


# Each generator returns (key, payload) so the partitioner spreads records
# across partitions sensibly without ad-hoc field probing at send time.


def gen_abd_balance(
    _iter: "Iterator[tuple[int, str, int]]" = iter(
        [
            # (day_offset_from_anchor, "HH:MM", balance)
            # Three suspicious gaps; after each one the balance jumps.
            (-36, "09:12", 42_000),
            (-35, "11:34", 36_500),
            (-34, "08:55", 812_000),  # where did this come from??
            (-33, "14:22", 790_000),
            (-32, "10:05", 835_000),
            (-31, "16:40", 748_000),
            (-30, "09:18", 771_000),
            # --- gap: -29 to -26 ---
            (-25, "12:50", 1_020_000),  # Sus, this can't be from selling fruit
            (-24, "08:30", 875_000),
            (-23, "15:15", 910_000),
            (-22, "11:00", 856_000),
            (-21, "09:45", 882_000),
            (-20, "13:20", 1_050_000),
            (-19, "10:10", 995_000),
            # --- gap: -18 to -15 ---
            (-14, "14:55", 1_875_000),  # Money laundering for sure
            (-13, "09:30", 1_792_000),
            (-12, "11:45", 1_940_000),
            # --- gap: -11 to -5 ---
            (-4, "08:20", 1_875_000),
            (-3, "12:35", 1_792_000),
            (-2, "10:00", 1_940_000),
        ]
    ),
    _state: dict = {"balance": None},
) -> tuple[str, dict]:
    try:
        day_offset, time_str, balance = next(_iter)
        hour, minute = int(time_str[:2]), int(time_str[3:])
        event_dt = (
            (ANCHOR_DATE if ANCHOR_DATE is not None else datetime.now(timezone.utc)) + timedelta(days=day_offset)
        ).replace(hour=hour, minute=minute, second=0, microsecond=0)
        _state["balance"] = balance
        return "player", {"timestamp": event_dt.isoformat(), "balance": balance}
    except StopIteration:
        pass

    # Continuous mode once the scripted narrative is exhausted: small drift
    # with the occasional "sus" lump-sum deposit so the topic never goes quiet.
    bal = _state["balance"] if _state["balance"] is not None else 1_900_000
    if random.random() < 0.03:
        bal += random.randint(200_000, 800_000)  # suspicious deposit
    else:
        bal += random.randint(-5_000, 8_000)  # normal fruit/turnip activity
    _state["balance"] = max(bal, 0)
    return "player", {"timestamp": now_iso(), "balance": _state["balance"]}


def gen_catch_log(
    _pools: list[tuple[str, list[str]]] = [
        ("fish", _load_names("fish.json")),
        ("bug", _load_names("bugs.json")),
        ("sea_creature", _load_names("sea_creatures.json")),
    ],
) -> tuple[str, dict]:
    item_type, pool = random.choice(_pools)
    return "player", {
        "item_type": item_type,
        "item_name": random.choice(pool),
        "timestamp": now_iso(),
    }


def gen_island_visitor(_state: dict = {"last_date": None}) -> tuple[str, dict] | None:
    date_str = datetime.now(timezone.utc).date().isoformat()
    if _state["last_date"] == date_str:
        return None
    _state["last_date"] = date_str
    visitor = ["Flick", "CJ"][hash(date_str) % 2]
    return "visitor", {"date": date_str, "name": visitor}


Generator = Callable[[], tuple[str, dict] | None]

GENERATORS: dict[str, Generator] = {
    "abd-balance": gen_abd_balance,
    "catch-log": gen_catch_log,
    "island-visitors": gen_island_visitor,
}


async def ensure_topics(bootstrap: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        new_topics = [NewTopic(t.name, num_partitions=t.partitions, replication_factor=1) for t in TOPICS]
        try:
            await admin.create_topics(new_topics)
            print(
                f"[seeder] created topics: {[t.name for t in TOPICS]}",
                flush=True,
            )
        except TopicAlreadyExistsError:
            print("[seeder] topics already exist; continuing", flush=True)
    finally:
        await admin.close()


# Shared per-topic counters used by the heartbeat task.
_counters: dict[str, int] = defaultdict(int)


async def produce_loop(producer: AIOKafkaProducer, spec: TopicSpec) -> None:
    """Continuously produce records for one topic.

    We use ``producer.send`` (fire-and-forget) so the loop is not blocked by
    leader-election retries on the very first record. The producer batches
    in the background; ``producer.flush`` in the heartbeat keeps the in-
    flight queue from growing unbounded.
    """
    if spec.messages_per_second <= 0:
        return
    generator = GENERATORS[spec.name]
    interval = 1.0 / spec.messages_per_second
    while True:
        try:
            result = generator()
            if result is None:
                await asyncio.sleep(interval * random.uniform(0.6, 1.4))
                continue
            key, payload = result
            await producer.send(
                spec.name,
                value=json.dumps(payload).encode("utf-8"),
                key=key.encode("utf-8"),
            )
            _counters[spec.name] += 1
        except Exception as exc:  # noqa: BLE001 - intentional broad catch
            print(
                f"[seeder] {spec.name}: send failed ({type(exc).__name__}: {exc})",
                flush=True,
            )
            # Brief backoff; the producer's own retry logic handles transient
            # broker errors, so we mostly land here for programming mistakes.
            await asyncio.sleep(1.0)
            continue
        await asyncio.sleep(interval * random.uniform(0.6, 1.4))


async def heartbeat(producer: AIOKafkaProducer, every_seconds: float = 30.0) -> None:
    """Periodically flush the producer and print per-topic counters."""
    last_snapshot: dict[str, int] = {t.name: 0 for t in TOPICS}
    while True:
        await asyncio.sleep(every_seconds)
        try:
            await producer.flush()
        except Exception as exc:  # noqa: BLE001
            print(f"[seeder] flush error: {type(exc).__name__}: {exc}", flush=True)
        snapshot = dict(_counters)
        deltas = {name: snapshot.get(name, 0) - last_snapshot.get(name, 0) for name in (t.name for t in TOPICS)}
        last_snapshot = snapshot
        line = " | ".join(f"{name}={snapshot.get(name, 0)} (+{deltas[name]})" for name in (t.name for t in TOPICS))
        print(f"[seeder] counters: {line}", flush=True)


async def main() -> None:
    random.seed(42)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "demo-kafka:19092")

    # demo-kafka can take a moment to accept connections after the container
    # reports as 'started', so retry a few times before giving up.
    for attempt in range(1, 31):
        try:
            await ensure_topics(bootstrap)
            break
        except Exception as exc:  # noqa: BLE001
            print(
                f"[seeder] kafka not ready (attempt {attempt}/30): {exc}",
                flush=True,
            )
            await asyncio.sleep(2)
    else:
        raise SystemExit("[seeder] kafka never became reachable")

    # Give the broker a moment to finish electing leaders for the topics we
    # just created. Without this we reliably hit NotLeaderForPartitionError
    # on the first produce; with it we typically dodge the error entirely.
    await asyncio.sleep(3.0)

    print("[seeder] starting producers", flush=True)

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        # Modest batching to keep the per-record round-trip cost down,
        # without hiding records from consumers for noticeably long.
        linger_ms=50,
        acks="all",
        request_timeout_ms=30_000,
    )
    await producer.start()
    try:
        await asyncio.gather(
            heartbeat(producer),
            *(produce_loop(producer, t) for t in TOPICS),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
