import uuid

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.agents.drift_agent import detect_drift
from app.agents.inference_agent import infer_schema
from app.agents.modeling_agent import build_data_model
from app.agents.quality_agent import generate_quality_rules, run_quality_checks
from app.database import get_db
from app.ingestion.csv_parser import parse_csv
from app.ingestion.json_parser import parse_json
from app.ingestion.pdf_parser import parse_pdf
from app.models.schema_models import (
    DataSource, DriftEvent, DriftSeverity, QualityResult, QualityStatus, SchemaVersion,
)
from app.schemas.pydantic_models import DataSourceOut, IngestResponse, QualityResultOut, SchemaVersionOut

router = APIRouter()

PARSERS = {"csv": parse_csv, "json": parse_json, "pdf": parse_pdf}


@router.post("/ingest", response_model=IngestResponse, summary="Ingest a file and run the full agent pipeline")
async def ingest(
    file: UploadFile = File(...),
    source_name: str = Form(default=""),
    db: AsyncSession = Depends(get_db),
):
    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in PARSERS:
        raise HTTPException(400, f"Unsupported file type: .{ext}. Supported: csv, json, pdf")

    content = await file.read()
    parsed = PARSERS[ext](content)
    name = source_name or file.filename or f"upload_{ext}"

    # --- find or create data source ---
    src_result = await db.execute(select(DataSource).where(DataSource.name == name))
    source: DataSource | None = src_result.scalar_one_or_none()

    if not source:
        source = DataSource(name=name, source_type=ext)
        db.add(source)
        await db.flush()

    # --- load previous schema version via explicit query (avoids lazy-load on relationship) ---
    prev_result = await db.execute(
        select(SchemaVersion)
        .where(SchemaVersion.source_id == source.id)
        .order_by(SchemaVersion.version.desc())
        .limit(1)
    )
    prev_version: SchemaVersion | None = prev_result.scalar_one_or_none()
    next_version_num = (prev_version.version + 1) if prev_version else 1

    # --- agent pipeline ---
    inferred_schema = await infer_schema(parsed)
    data_model = await build_data_model(inferred_schema)
    quality_rules = await generate_quality_rules(inferred_schema)
    quality_check_results = run_quality_checks(parsed, quality_rules)

    # --- drift detection ---
    drift_severity = DriftSeverity.NONE
    drift_summary = None
    drift_events_data = []

    if prev_version:
        drift_result = await detect_drift(prev_version.inferred_schema, inferred_schema)
        drift_severity = DriftSeverity(drift_result.overall_severity)
        drift_summary = drift_result.summary
        drift_events_data = drift_result.events

    # --- persist schema version ---
    schema_version = SchemaVersion(
        source_id=source.id,
        version=next_version_num,
        inferred_schema=inferred_schema,
        data_model=data_model,
        drift_severity=drift_severity,
        drift_summary=drift_summary,
        quality_rules=quality_rules,
        raw_sample={"rows": parsed.rows[:10]},
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

    # --- persist quality results ---
    quality_result_records = []
    for qr in quality_check_results:
        record = QualityResult(
            source_id=source.id,
            schema_version_id=schema_version.id,
            rule_name=qr["rule_name"],
            status=QualityStatus(qr["status"]),
            message=qr.get("message"),
            details=qr.get("details"),
        )
        db.add(record)
        quality_result_records.append(record)

    await db.commit()

    # Reload everything fresh after commit to avoid lazy-load in async context
    sv_result = await db.execute(
        select(SchemaVersion)
        .where(SchemaVersion.id == schema_version.id)
        .options(selectinload(SchemaVersion.drift_events))
    )
    fresh_version = sv_result.scalar_one()

    src_result = await db.execute(
        select(DataSource)
        .where(DataSource.id == source.id)
        .options(selectinload(DataSource.schema_versions).selectinload(SchemaVersion.drift_events))
    )
    fresh_source = src_result.scalar_one()

    qr_result = await db.execute(
        select(QualityResult).where(QualityResult.schema_version_id == fresh_version.id)
    )
    fresh_quality = qr_result.scalars().all()

    return IngestResponse(
        source=DataSourceOut.model_validate(fresh_source),
        latest_version=SchemaVersionOut.model_validate(fresh_version),
        quality_results=[QualityResultOut.model_validate(r) for r in fresh_quality],
        drift_detected=drift_severity != DriftSeverity.NONE,
        drift_severity=drift_severity.value,
    )


@router.get("/sources", response_model=list[DataSourceOut], summary="List all data sources")
async def list_sources(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(DataSource).options(
            selectinload(DataSource.schema_versions).selectinload(SchemaVersion.drift_events)
        )
    )
    return [DataSourceOut.model_validate(s) for s in result.scalars().all()]


@router.get("/sources/{source_id}", response_model=DataSourceOut, summary="Get a source and all schema versions")
async def get_source(source_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(DataSource)
        .where(DataSource.id == source_id)
        .options(selectinload(DataSource.schema_versions).selectinload(SchemaVersion.drift_events))
    )
    source = result.scalar_one_or_none()
    if not source:
        raise HTTPException(404, "Source not found")
    return DataSourceOut.model_validate(source)


@router.get("/sources/{source_id}/quality", response_model=list[QualityResultOut], summary="Get quality results for a source")
async def get_quality(source_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(QualityResult)
        .where(QualityResult.source_id == source_id)
        .order_by(QualityResult.created_at.desc())
    )
    return [QualityResultOut.model_validate(r) for r in result.scalars().all()]


@router.get("/sources/{source_id}/ddl", summary="Generate CREATE TABLE DDL from latest data model")
async def get_ddl(source_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(SchemaVersion)
        .where(SchemaVersion.source_id == source_id)
        .order_by(SchemaVersion.version.desc())
        .limit(1)
    )
    version = result.scalar_one_or_none()
    if not version or not version.data_model:
        raise HTTPException(404, "No data model found for this source")

    ddl_statements = []
    for table in version.data_model.get("tables", []):
        cols = []
        for col in table.get("columns", []):
            parts = [f"  {col['name']} {col['sql_type']}"]
            if col.get("primary_key"):
                parts.append("PRIMARY KEY")
            if not col.get("nullable", True):
                parts.append("NOT NULL")
            if col.get("unique"):
                parts.append("UNIQUE")
            if col.get("default") is not None:
                parts.append(f"DEFAULT {col['default']}")
            cols.append(" ".join(parts))

        for fk in table.get("foreign_keys", []):
            cols.append(
                f"  FOREIGN KEY ({fk['column']}) REFERENCES {fk['references_table']}({fk['references_column']})"
                f" ON DELETE {fk.get('on_delete', 'RESTRICT')}"
            )

        ddl = f"CREATE TABLE IF NOT EXISTS {table['table_name']} (\n" + ",\n".join(cols) + "\n);"
        ddl_statements.append(ddl)

    return {"ddl": "\n\n".join(ddl_statements), "version": version.version}
