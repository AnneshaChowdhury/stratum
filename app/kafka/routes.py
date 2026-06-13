import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.kafka.consumer import is_consuming, start_consumer, stop_consumer
from app.models.schema_models import KafkaTopicSource

router = APIRouter(prefix="/kafka", tags=["Kafka"])


class RegisterTopicRequest(BaseModel):
    topic: str
    bootstrap_servers: str = ""
    auto_start: bool = True


class KafkaTopicOut(BaseModel):
    id: uuid.UUID
    topic: str
    bootstrap_servers: str
    is_active: bool
    messages_consumed: int
    last_offset: int | None
    source_id: uuid.UUID | None

    class Config:
        from_attributes = True


@router.post("/topics", response_model=KafkaTopicOut, summary="Register a Kafka topic and start consuming")
async def register_topic(req: RegisterTopicRequest, db: AsyncSession = Depends(get_db)):
    servers = req.bootstrap_servers or settings.kafka_bootstrap_servers

    # check if already registered
    result = await db.execute(
        select(KafkaTopicSource).where(KafkaTopicSource.topic == req.topic)
    )
    kt = result.scalar_one_or_none()

    if not kt:
        kt = KafkaTopicSource(
            topic=req.topic,
            bootstrap_servers=servers,
            is_active=False,
        )
        db.add(kt)
        await db.flush()

    if req.auto_start:
        await start_consumer(req.topic, servers)
        kt.is_active = True

    await db.commit()
    await db.refresh(kt)
    return KafkaTopicOut.model_validate(kt)


@router.get("/topics", response_model=list[KafkaTopicOut], summary="List all registered Kafka topics")
async def list_topics(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(KafkaTopicSource))
    topics = result.scalars().all()
    # sync live status with in-memory consumer state
    for t in topics:
        t.is_active = is_consuming(t.topic)
    return [KafkaTopicOut.model_validate(t) for t in topics]


@router.post("/topics/{topic}/start", response_model=KafkaTopicOut, summary="Start consuming a topic")
async def start_topic(topic: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(KafkaTopicSource).where(KafkaTopicSource.topic == topic)
    )
    kt = result.scalar_one_or_none()
    if not kt:
        raise HTTPException(404, f"Topic '{topic}' not registered. Call POST /kafka/topics first.")

    await start_consumer(topic, kt.bootstrap_servers)
    kt.is_active = True
    await db.commit()
    await db.refresh(kt)
    return KafkaTopicOut.model_validate(kt)


@router.post("/topics/{topic}/stop", response_model=KafkaTopicOut, summary="Stop consuming a topic")
async def stop_topic(topic: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(KafkaTopicSource).where(KafkaTopicSource.topic == topic)
    )
    kt = result.scalar_one_or_none()
    if not kt:
        raise HTTPException(404, f"Topic '{topic}' not registered.")

    await stop_consumer(topic)
    kt.is_active = False
    await db.commit()
    await db.refresh(kt)
    return KafkaTopicOut.model_validate(kt)


@router.delete("/topics/{topic}", summary="Unregister a Kafka topic")
async def delete_topic(topic: str, db: AsyncSession = Depends(get_db)):
    await stop_consumer(topic)
    result = await db.execute(
        select(KafkaTopicSource).where(KafkaTopicSource.topic == topic)
    )
    kt = result.scalar_one_or_none()
    if kt:
        await db.delete(kt)
        await db.commit()
    return {"deleted": topic}


@router.get("/topics/{topic}/status", summary="Get live consumer status for a topic")
async def topic_status(topic: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(KafkaTopicSource).where(KafkaTopicSource.topic == topic)
    )
    kt = result.scalar_one_or_none()
    if not kt:
        raise HTTPException(404, f"Topic '{topic}' not registered.")
    return {
        "topic": topic,
        "is_consuming": is_consuming(topic),
        "messages_consumed": kt.messages_consumed,
        "last_offset": kt.last_offset,
        "last_seen_at": kt.last_seen_at,
        "source_id": kt.source_id,
    }
