import uuid
from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import JSON, DateTime, Enum, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class DriftSeverity(str, PyEnum):
    NONE = "none"
    ADDITIVE = "additive"
    RISKY = "risky"
    BREAKING = "breaking"


class QualityStatus(str, PyEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


# native_enum=False stores as VARCHAR to avoid asyncpg codec issues with pg native ENUMs
_drift_col = Enum(DriftSeverity, native_enum=False)
_quality_col = Enum(QualityStatus, native_enum=False)


class DataSource(Base):
    __tablename__ = "data_sources"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    schema_versions: Mapped[list["SchemaVersion"]] = relationship(back_populates="source", order_by="SchemaVersion.version")
    quality_results: Mapped[list["QualityResult"]] = relationship(back_populates="source")


class SchemaVersion(Base):
    __tablename__ = "schema_versions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("data_sources.id"), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    inferred_schema: Mapped[dict] = mapped_column(JSON, nullable=False)
    data_model: Mapped[dict] = mapped_column(JSON, nullable=True)
    drift_severity: Mapped[str] = mapped_column(Enum(DriftSeverity, native_enum=False), default=DriftSeverity.NONE)
    drift_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    quality_rules: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    raw_sample: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    source: Mapped["DataSource"] = relationship(back_populates="schema_versions")
    drift_events: Mapped[list["DriftEvent"]] = relationship(back_populates="schema_version")


class DriftEvent(Base):
    __tablename__ = "drift_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    schema_version_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schema_versions.id"), nullable=False)
    field_name: Mapped[str] = mapped_column(String(255), nullable=False)
    change_type: Mapped[str] = mapped_column(String(50))
    old_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str] = mapped_column(Enum(DriftSeverity, native_enum=False))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    schema_version: Mapped["SchemaVersion"] = relationship(back_populates="drift_events")


class QualityResult(Base):
    __tablename__ = "quality_results"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("data_sources.id"), nullable=False)
    schema_version_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schema_versions.id"), nullable=False)
    rule_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(Enum(QualityStatus, native_enum=False))
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    source: Mapped["DataSource"] = relationship(back_populates="quality_results")
