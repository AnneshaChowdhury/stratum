import uuid
from datetime import datetime

from pydantic import BaseModel


class FieldSchema(BaseModel):
    name: str
    data_type: str          # string, integer, float, boolean, date, timestamp, json
    semantic_type: str      # customer_id, email, monetary_amount, event_timestamp, etc.
    nullable: bool = True
    confidence: float = 1.0  # 0.0–1.0; low confidence fields are flagged for review


class RelationshipSchema(BaseModel):
    from_field: str
    to_table: str
    to_field: str
    relationship_type: str  # one_to_many, many_to_one, many_to_many


class TableSchema(BaseModel):
    table_name: str
    fields: list[FieldSchema]
    relationships: list[RelationshipSchema] = []
    description: str = ""


class InferredSchema(BaseModel):
    tables: list[TableSchema]
    raw_fields: list[FieldSchema] = []
    source_description: str = ""


class DriftEventOut(BaseModel):
    field_name: str
    change_type: str
    old_value: str | None
    new_value: str | None
    severity: str

    class Config:
        from_attributes = True


class SchemaVersionOut(BaseModel):
    id: uuid.UUID
    version: int
    inferred_schema: dict
    data_model: dict | None
    drift_severity: str
    drift_summary: str | None
    quality_rules: dict | None
    created_at: datetime
    drift_events: list[DriftEventOut] = []

    class Config:
        from_attributes = True


class DataSourceOut(BaseModel):
    id: uuid.UUID
    name: str
    source_type: str
    created_at: datetime
    schema_versions: list[SchemaVersionOut] = []

    class Config:
        from_attributes = True


class QualityResultOut(BaseModel):
    id: uuid.UUID
    rule_name: str
    status: str
    message: str | None
    details: dict | None
    created_at: datetime

    class Config:
        from_attributes = True


class IngestResponse(BaseModel):
    source: DataSourceOut
    latest_version: SchemaVersionOut
    quality_results: list[QualityResultOut]
    drift_detected: bool
    drift_severity: str
