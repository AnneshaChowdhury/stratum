from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://schemamorph@localhost:5432/schemamorph"
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"
    max_sample_rows: int = 100
    max_pdf_pages: int = 10


settings = Settings()
