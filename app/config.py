from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://stratum@localhost:5432/stratum"
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"
    max_sample_rows: int = 100
    max_pdf_pages: int = 10
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "stratum-agent"
    kafka_batch_size: int = 50       # messages to buffer before running inference
    kafka_batch_timeout_ms: int = 5000  # flush batch after this ms even if not full


settings = Settings()
