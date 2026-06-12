from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="SchemaMorph",
    description="AI agent that ingests unstructured data, infers schemas iteratively, detects drift, and runs data quality checks — powered by Llama 3.3 70B via Groq (free).",
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


@app.get("/health")
async def health():
    from app.config import settings
    return {"status": "ok", "model": settings.groq_model, "backend": "groq"}
