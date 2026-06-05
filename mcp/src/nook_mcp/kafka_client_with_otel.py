"""Kafka helpers for ``nook-mcp`` — the *instrumented* "after".

This module is :mod:`nook_mcp.kafka_client` with the section 4 messaging
instrumentation applied: a module-level named tracer, a ``_messaging_attrs``
helper that builds the OTel messaging-semconv attribute bag, and each
public helper wrapped in a ``start_as_current_span`` block that emits a
``<topic> receive`` / ``<topic> publish`` span and records exceptions on
the error path.

The public function signatures and return values are unchanged, so the
tools and :mod:`nook_mcp.server` can use any of the three kafka_client
modules without other changes.

Spans emitted:
  * ``<topic> receive`` — ``peek_topic_messages`` / ``query_topic_messages``;
    carries ``messaging.system``, ``messaging.operation.name=receive``,
    ``messaging.destination.name`` and ``messaging.batch.message_count``.
  * ``<topic> publish`` — ``publish_event_to_topic``; additionally carries
    ``messaging.kafka.destination.partition`` and
    ``messaging.kafka.message.offset`` once the broker acknowledges.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode


_tracer = trace.get_tracer(__name__)


def _messaging_attrs(topic: str, operation: str) -> dict[str, Any]:
    """Standard OTel messaging-semconv attributes for a Kafka op."""
    return {
        "messaging.system": "kafka",
        "messaging.operation.name": operation,
        "messaging.destination.name": topic,
    }


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "demo-kafka:19092")


@asynccontextmanager
async def admin_client() -> AsyncIterator[AIOKafkaAdminClient]:
    client = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers())
    await client.start()
    try:
        yield client
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Record decoding
# ---------------------------------------------------------------------------


def _decode_record(record: Any) -> dict:
    """Decode a single Kafka record into a JSON-friendly dict.

    The body is parsed as JSON when possible; otherwise the raw bytes are
    surfaced as a string so the tool always returns something the LLM can
    reason about.
    """
    try:
        value: Any = json.loads(record.value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        value = {"raw": record.value.decode("utf-8", errors="replace") if record.value else None}
    return {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "timestamp_ms": record.timestamp,
        "key": record.key.decode("utf-8") if record.key else None,
        "value": value,
    }


def _iso_to_ms(iso: str) -> int:
    """Convert an ISO-8601 timestamp into Unix epoch milliseconds."""
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


# Topic name prefixes that we never want to surface to the LLM. Kafka
# internal topics start with '_'; fast-data-dev's bundled Kafka Connect
# creates 'connect-configs', 'connect-offsets', 'connect-statuses' even
# when sample data is disabled.
_HIDDEN_PREFIXES: tuple[str, ...] = ("_", "connect-")

# fast-data-dev pre-creates these topics on startup even with
# RUNNING_SAMPLEDATA=0. They have no data but pollute the topic list.
_HIDDEN_EXACT_NAMES: frozenset[str] = frozenset(
    {
        "backblaze_smart",
        "logs_broker",
        "nyc_yellow_taxi_trip_data",
        "sea_vessel_position_reports",
        "telecom_italia_data",
        "telecom_italia_grid",
    }
)


def _is_visible(name: str) -> bool:
    if name.startswith(_HIDDEN_PREFIXES):
        return False
    if name in _HIDDEN_EXACT_NAMES:
        return False
    return True


async def list_topic_names() -> list[str]:
    """Return user topic names on the cluster, sorted alphabetically.

    Internal Kafka topics (``_`` prefix), Kafka Connect bookkeeping topics
    (``connect-`` prefix) and a known set of fast-data-dev sample topics
    are filtered out so the workshop's intentional topics are easy to spot.
    """
    async with admin_client() as admin:
        topics = await admin.list_topics()
    return sorted(t for t in topics if _is_visible(t))


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


async def _drain(
    consumer: AIOKafkaConsumer,
    limit: int,
    deadline_seconds: float,
    key_filter: str | None = None,
) -> list[dict]:
    """Pull records from a consumer until ``limit`` matches or the deadline."""
    messages: list[dict] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + deadline_seconds
    while len(messages) < limit and loop.time() < deadline:
        remaining_ms = max(50, int((deadline - loop.time()) * 1000))
        batch = await consumer.getmany(timeout_ms=min(remaining_ms, 500))
        if not batch:
            continue
        for records in batch.values():
            for record in records:
                msg = _decode_record(record)
                if key_filter is not None and msg["key"] != key_filter:
                    continue
                messages.append(msg)
                if len(messages) >= limit:
                    return messages
    return messages


async def peek_topic_messages(topic: str, limit: int = 10) -> list[dict]:
    """Return up to ``limit`` of the most recent messages on ``topic``.

    A fresh consumer with no group is created per call. We assign every
    partition explicitly, seek to ``end - (limit / num_partitions)`` on each,
    and read until we have enough records or 3 seconds elapse. Records are
    returned newest-first.
    """
    with _tracer.start_as_current_span(
        f"{topic} receive",
        attributes=_messaging_attrs(topic, "receive"),
    ) as span:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers(),
            enable_auto_commit=False,
            group_id=None,
        )
        await consumer.start()
        try:
            partitions = consumer.partitions_for_topic(topic) or set()
            if not partitions:
                span.set_attribute("messaging.batch.message_count", 0)
                return []
            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            consumer.assign(tps)

            end_offsets = await consumer.end_offsets(tps)
            beginning_offsets = await consumer.beginning_offsets(tps)
            per_partition = max(1, (limit + len(tps) - 1) // len(tps))
            for tp in tps:
                target = max(beginning_offsets[tp], end_offsets[tp] - per_partition)
                consumer.seek(tp, target)

            messages = await _drain(consumer, limit, deadline_seconds=3.0)
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            await consumer.stop()

        messages.sort(key=lambda m: m["timestamp_ms"], reverse=True)
        messages = messages[:limit]
        span.set_attribute("messaging.batch.message_count", len(messages))
        return messages


async def query_topic_messages(
    topic: str,
    key_filter: str | None = None,
    since_iso: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Filter messages on ``topic`` by key and/or since-timestamp.

    If ``since_iso`` is given we use ``offsets_for_times`` to seek to the
    first record at or after that timestamp. Otherwise we read the last
    ``limit * 2`` records on each partition and let the key filter trim.
    """
    with _tracer.start_as_current_span(
        f"{topic} receive",
        attributes=_messaging_attrs(topic, "receive"),
    ) as span:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers(),
            enable_auto_commit=False,
            group_id=None,
        )
        await consumer.start()
        try:
            partitions = consumer.partitions_for_topic(topic) or set()
            if not partitions:
                span.set_attribute("messaging.batch.message_count", 0)
                return []
            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            consumer.assign(tps)

            if since_iso is not None:
                since_ms = _iso_to_ms(since_iso)
                offsets = await consumer.offsets_for_times({tp: since_ms for tp in tps})
                end_offsets = await consumer.end_offsets(tps)
                for tp, ots in offsets.items():
                    if ots is not None:
                        consumer.seek(tp, ots.offset)
                    else:
                        consumer.seek(tp, end_offsets[tp])
            else:
                end_offsets = await consumer.end_offsets(tps)
                beginning_offsets = await consumer.beginning_offsets(tps)
                over_fetch = max(1, (limit * 2 + len(tps) - 1) // len(tps))
                for tp in tps:
                    target = max(beginning_offsets[tp], end_offsets[tp] - over_fetch)
                    consumer.seek(tp, target)

            messages = await _drain(consumer, limit, deadline_seconds=3.0, key_filter=key_filter)
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            await consumer.stop()

        messages.sort(key=lambda m: m["timestamp_ms"], reverse=True)
        messages = messages[:limit]
        span.set_attribute("messaging.batch.message_count", len(messages))
        return messages


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


async def publish_event_to_topic(
    topic: str,
    payload: dict,
    key: str | None = None,
) -> dict:
    """Produce a single JSON record and return its broker metadata."""
    with _tracer.start_as_current_span(
        f"{topic} publish",
        attributes=_messaging_attrs(topic, "publish"),
    ) as span:
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers(),
            acks="all",
        )
        await producer.start()
        try:
            metadata = await producer.send_and_wait(
                topic,
                value=json.dumps(payload).encode("utf-8"),
                key=key.encode("utf-8") if key else None,
            )
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise
        finally:
            await producer.stop()
        span.set_attribute("messaging.kafka.destination.partition", metadata.partition)
        span.set_attribute("messaging.kafka.message.offset", metadata.offset)
        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "timestamp_ms": metadata.timestamp,
        }
