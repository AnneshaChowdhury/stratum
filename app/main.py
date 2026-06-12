from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select

from app.api.routes import router
from app.database import AsyncSessionLocal, init_db
from app.kafka.routes import router as kafka_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await _resume_kafka_consumers()
    yield


async def _resume_kafka_consumers() -> None:
    """On startup, restart any topics that were active before the last shutdown."""
    from app.kafka.consumer import start_consumer
    from app.models.schema_models import KafkaTopicSource

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(KafkaTopicSource).where(KafkaTopicSource.is_active == True)  # noqa: E712
        )
        active = result.scalars().all()
        for kt in active:
            await start_consumer(kt.topic, kt.bootstrap_servers)


app = FastAPI(
    title="Stratum",
    description="AI agent that ingests unstructured data (CSV, JSON, PDF, Kafka), infers schemas iteratively, detects drift, and runs data quality checks — powered by Llama 3.3 70B via Groq.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")
app.include_router(kafka_router, prefix="/api/v1")


@app.get("/health")
async def health():
    from app.kafka.consumer import _active_consumers
    from app.config import settings
    return {
        "status": "ok",
        "model": settings.groq_model,
        "backend": "groq",
        "kafka_consumers_active": len(_active_consumers),
    }
