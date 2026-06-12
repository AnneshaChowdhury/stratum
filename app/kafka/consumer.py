"""
Kafka Consumer — subscribes to a topic, batches messages, and feeds them
through the full Stratum agent pipeline: inference → modeling → drift → quality.

Each batch is treated like a file upload — the same agents run, schema versions
are tracked, and drift is detected as the message schema evolves over time.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from sqlalchemy import select

from app.agents.drift_agent import detect_drift
from app.agents.inference_agent import infer_schema
from app.agents.modeling_agent import build_data_model
from app.agents.quality_agent import generate_quality_rules, run_quality_checks
from app.config import settings
from app.database import AsyncSessionLocal
from app.ingestion.base import ParsedData
from app.models.schema_models import (
    DataSource, DriftEvent, DriftSeverity, KafkaTopicSource,
    QualityResult, QualityStatus, SchemaVersion,
)

logger = logging.getLogger("stratum.kafka")

# topic → asyncio.Task — tracks running consumers
_active_consumers: dict[str, asyncio.Task] = {}


def is_consuming(topic: str) -> bool:
    task = _active_consumers.get(topic)
    return task is not None and not task.done()


async def start_consumer(topic: str, bootstrap_servers: str | None = None) -> None:
    """Start a background consumer task for a topic. No-op if already running."""
    if is_consuming(topic):
        logger.info("Consumer for %s already running", topic)
        return
    servers = bootstrap_servers or settings.kafka_bootstrap_servers
    task = asyncio.create_task(
        _consume_loop(topic, servers),
        name=f"kafka-consumer-{topic}",
    )
    _active_consumers[topic] = task
    logger.info("Started consumer for topic '%s' on %s", topic, servers)


async def stop_consumer(topic: str) -> None:
    """Cancel the background consumer task for a topic."""
    task = _active_consumers.pop(topic, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    logger.info("Stopped consumer for topic '%s'", topic)


async def _consume_loop(topic: str, bootstrap_servers: str) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        value_deserializer=_deserialize,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    try:
        await consumer.start()
        logger.info("Consumer connected to topic '%s'", topic)

        batch: list[dict] = []
        last_flush = asyncio.get_event_loop().time()

        async for msg in consumer:
            if msg.value is None:
                continue

            value = msg.value
            if isinstance(value, dict):
                batch.append(value)
            elif isinstance(value, list):
                batch.extend(v for v in value if isinstance(v, dict))

            elapsed_ms = (asyncio.get_event_loop().time() - last_flush) * 1000
            should_flush = (
                len(batch) >= settings.kafka_batch_size
                or elapsed_ms >= settings.kafka_batch_timeout_ms
            )

            if should_flush and batch:
                await _process_batch(topic, bootstrap_servers, batch, msg.offset)
                batch = []
                last_flush = asyncio.get_event_loop().time()

    except asyncio.CancelledError:
        logger.info("Consumer for '%s' cancelled", topic)
    except KafkaError as exc:
        logger.error("Kafka error on topic '%s': %s", topic, exc)
    finally:
        await consumer.stop()
        await _mark_inactive(topic)


async def _process_batch(topic: str, bootstrap_servers: str, rows: list[dict], offset: int) -> None:
    """Run the full Stratum agent pipeline on a batch of Kafka messages."""
    all_keys: set[str] = set()
    for row in rows:
        all_keys.update(row.keys())

    parsed = ParsedData(
        rows=rows[: settings.max_sample_rows],
        all_keys=all_keys,
        row_count=len(rows),
        source_type="kafka",
    )

    async with AsyncSessionLocal() as db:
        # find or create DataSource for this topic
        src_result = await db.execute(
            select(DataSource).where(DataSource.name == f"kafka:{topic}")
        )
        source = src_result.scalar_one_or_none()
        if not source:
            source = DataSource(name=f"kafka:{topic}", source_type="kafka")
            db.add(source)
            await db.flush()

        # find previous schema version
        prev_result = await db.execute(
            select(SchemaVersion)
            .where(SchemaVersion.source_id == source.id)
            .order_by(SchemaVersion.version.desc())
            .limit(1)
        )
        prev_version = prev_result.scalar_one_or_none()
        next_version_num = (prev_version.version + 1) if prev_version else 1

        # agent pipeline
        try:
            inferred_schema = await infer_schema(parsed)
            data_model = await build_data_model(inferred_schema)
            quality_rules = await generate_quality_rules(inferred_schema)
            quality_check_results = run_quality_checks(parsed, quality_rules)
        except Exception as exc:
            logger.error("Agent pipeline failed for topic '%s': %s", topic, exc)
            return

        # drift detection
        drift_severity = DriftSeverity.NONE
        drift_summary = None
        drift_events_data = []

        if prev_version:
            drift_result = await detect_drift(prev_version.inferred_schema, inferred_schema)
            drift_severity = DriftSeverity(drift_result.overall_severity)
            drift_summary = drift_result.summary
            drift_events_data = drift_result.events

            if drift_severity == DriftSeverity.BREAKING:
                logger.warning(
                    "BREAKING schema drift on topic '%s': %s", topic, drift_summary
                )
            elif drift_severity == DriftSeverity.RISKY:
                logger.warning(
                    "Risky schema drift on topic '%s': %s", topic, drift_summary
                )

        # persist schema version
        schema_version = SchemaVersion(
            source_id=source.id,
            version=next_version_num,
            inferred_schema=inferred_schema,
            data_model=data_model,
            drift_severity=drift_severity,
            drift_summary=drift_summary,
            quality_rules=quality_rules,
            raw_sample={"rows": rows[:10]},
        )
        db.add(schema_version)
        await db.flush()

        for event in drift_events_data:
            db.add(DriftEvent(
                schema_version_id=schema_version.id,
                field_name=event.get("field_name", "unknown"),
                change_type=event.get("change_type", "unknown"),
                old_value=str(event.get("old_value")) if event.get("old_value") else None,
                new_value=str(event.get("new_value")) if event.get("new_value") else None,
                severity=DriftSeverity(event.get("severity", "none")),
            ))

        for qr in quality_check_results:
            db.add(QualityResult(
                source_id=source.id,
                schema_version_id=schema_version.id,
                rule_name=qr["rule_name"],
                status=QualityStatus(qr["status"]),
                message=qr.get("message"),
                details=qr.get("details"),
            ))

        # update KafkaTopicSource stats
        kt_result = await db.execute(
            select(KafkaTopicSource).where(KafkaTopicSource.topic == topic)
        )
        kt = kt_result.scalar_one_or_none()
        if kt:
            kt.messages_consumed += len(rows)
            kt.last_offset = offset
            kt.last_seen_at = datetime.now(timezone.utc)

        await db.commit()

        logger.info(
            "topic='%s' batch=%d version=%d drift=%s quality_checks=%d",
            topic, len(rows), next_version_num, drift_severity.value,
            len(quality_check_results),
        )


async def _mark_inactive(topic: str) -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(KafkaTopicSource).where(KafkaTopicSource.topic == topic)
        )
        kt = result.scalar_one_or_none()
        if kt:
            kt.is_active = False
            await db.commit()


def _deserialize(value: bytes) -> dict | list | None:
    try:
        return json.loads(value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
