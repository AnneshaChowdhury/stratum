"""
dbt Generation Agent — produces staging SQL models, sources.yml, and
schema.yml with tests from Stratum's inferred schema and quality rules.

No LLM call needed: the inferred schema already has types, semantic labels,
and nullability; the quality rules map directly to dbt built-in tests.
"""

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import yaml


# ── type casting ─────────────────────────────────────────────────────────────

_SEMANTIC_CASTS: dict[str, str] = {
    "monetary": "numeric(10,2)",
    "amount":   "numeric(10,2)",
    "price":    "numeric(10,2)",
    "spend":    "numeric(10,2)",
    "cost":     "numeric(10,2)",
    "revenue":  "numeric(10,2)",
}

_SQL_TYPE_CASTS: dict[str, str] = {
    "INTEGER":   "integer",
    "BIGINT":    "bigint",
    "BOOLEAN":   "boolean",
    "DATE":      "date",
    "TIMESTAMP": "timestamp",
    "JSONB":     "jsonb",
}

_DATA_TYPE_CASTS: dict[str, str] = {
    "integer":   "integer",
    "float":     "numeric",
    "boolean":   "boolean",
    "date":      "date",
    "timestamp": "timestamp",
    "json":      "jsonb",
}


def _cast_expr(field: dict, model_col: dict | None) -> str:
    name = field["name"]
    semantic = field.get("semantic_type", "").lower()
    data_type = field.get("data_type", "string")
    sql_type = (model_col.get("sql_type", "") if model_col else "").upper()

    # semantic overrides win first (monetary amounts need precise numeric)
    for keyword, cast in _SEMANTIC_CASTS.items():
        if keyword in semantic:
            return f"{name}::{cast} as {name}"

    # sql_type from modeling agent
    for prefix, cast in _SQL_TYPE_CASTS.items():
        if sql_type.startswith(prefix):
            return f"{name}::{cast} as {name}"
    if sql_type.startswith("NUMERIC"):
        return f"{name}::{sql_type.lower()} as {name}"

    # fall back to data_type from inference agent
    cast = _DATA_TYPE_CASTS.get(data_type)
    if cast:
        return f"{name}::{cast} as {name}"

    return name  # string / varchar — no cast


# ── descriptions ─────────────────────────────────────────────────────────────

_SEMANTIC_DESCRIPTIONS: dict[str, str] = {
    "customer_id":    "Unique customer identifier",
    "order_id":       "Unique order identifier",
    "email":          "Email address",
    "monetary":       "Monetary value",
    "amount":         "Monetary value",
    "price":          "Unit price",
    "spend":          "Spend amount",
    "event_timestamp":"Event timestamp",
    "status":         "Status indicator",
    "phone":          "Phone number",
    "free_text":      "Free-form text",
    "date":           "Date value",
    "timestamp":      "Timestamp value",
}


def _describe(semantic_type: str) -> str:
    lower = semantic_type.lower()
    for key, desc in _SEMANTIC_DESCRIPTIONS.items():
        if key in lower:
            return desc
    return semantic_type.replace("_", " ").capitalize()


# ── dbt test derivation ───────────────────────────────────────────────────────

def _tests_for(field_name: str, quality_rules: dict) -> list[str]:
    tests: list[str] = []
    seen: set[str] = set()
    for rule in quality_rules.get("rules", []):
        if rule.get("field") != field_name:
            continue
        t = rule.get("rule_type", "")
        if t == "not_null" and "not_null" not in seen:
            tests.append("not_null")
            seen.add("not_null")
        elif t == "unique" and "unique" not in seen:
            tests.append("unique")
            seen.add("unique")
    return tests


# ── main generation ───────────────────────────────────────────────────────────

def generate_dbt_artifacts(
    inferred_schema: dict,
    data_model: dict,
    quality_rules: dict,
    source_name: str = "raw",
) -> dict[str, str]:
    """
    Returns {filename: content} for all dbt artifacts.

    Files produced per table:
      models/staging/sources.yml            (shared, one entry per table)
      models/staging/stg_<table>.sql
      models/staging/stg_<table>.yml
    """
    # model column lookup: table_name → col_name → col dict
    model_lookup: dict[str, dict[str, dict]] = {}
    for t in data_model.get("tables", []):
        model_lookup[t["table_name"]] = {c["name"]: c for c in t.get("columns", [])}

    artifacts: dict[str, str] = {}
    source_tables_yaml: list[dict] = []

    for table in inferred_schema.get("tables", []):
        tname = table["table_name"]
        fields = table.get("fields", [])
        col_lookup = model_lookup.get(tname, {})

        # ── sources.yml entry ─────────────────────────────────────────────
        source_tables_yaml.append({
            "name": tname,
            "description": table.get("description", ""),
            "columns": [
                {"name": f["name"], "description": _describe(f.get("semantic_type", ""))}
                for f in fields
            ],
        })

        # ── stg_<table>.sql ───────────────────────────────────────────────
        cast_lines = ",\n".join(
            "        " + _cast_expr(f, col_lookup.get(f["name"]))
            for f in fields
        )
        sql = (
            f"with source as (\n"
            f"    select * from {{{{ source('{source_name}', '{tname}') }}}}\n"
            f"),\n"
            f"renamed as (\n"
            f"    select\n"
            f"{cast_lines}\n"
            f"    from source\n"
            f")\n"
            f"select * from renamed\n"
        )
        artifacts[f"models/staging/stg_{tname}.sql"] = sql

        # ── stg_<table>.yml ───────────────────────────────────────────────
        schema_cols = []
        for f in fields:
            col: dict = {
                "name": f["name"],
                "description": _describe(f.get("semantic_type", "")),
            }
            tests = _tests_for(f["name"], quality_rules)
            if tests:
                col["tests"] = tests
            schema_cols.append(col)

        schema_doc = {
            "version": 2,
            "models": [{
                "name": f"stg_{tname}",
                "description": (
                    f"Staged {tname} records with type casting and data quality tests. "
                    f"Generated by Stratum from {table.get('description', tname)}."
                ),
                "columns": schema_cols,
            }],
        }
        artifacts[f"models/staging/stg_{tname}.yml"] = yaml.dump(
            schema_doc, default_flow_style=False, sort_keys=False, allow_unicode=True
        )

    # ── sources.yml (all tables for this source) ──────────────────────────
    sources_doc = {
        "version": 2,
        "sources": [{
            "name": source_name,
            "description": inferred_schema.get("source_description", "Raw data ingested by Stratum"),
            "tables": source_tables_yaml,
        }],
    }
    artifacts["models/staging/sources.yml"] = yaml.dump(
        sources_doc, default_flow_style=False, sort_keys=False, allow_unicode=True
    )

    return artifacts


def artifacts_to_zip(artifacts: dict[str, str]) -> bytes:
    buf = BytesIO()
    with ZipFile(buf, "w", ZIP_DEFLATED) as zf:
        for path, content in artifacts.items():
            zf.writestr(path, content)
    return buf.getvalue()
