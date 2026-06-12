"""
Data Modeling Agent — takes an inferred schema and produces a full
relational data model: DDL-ready table definitions, primary keys,
foreign keys, indexes, and a description of design decisions.
"""

import json

from app.agents.ollama_client import chat, extract_json

SYSTEM = """You are a senior data architect.
Given an inferred schema, produce a clean, normalized relational data model.
Always respond with valid JSON only."""

MODELING_PROMPT = """Given the following inferred schema, produce a production-ready relational data model.

Inferred schema:
{schema}

Return a JSON object:
{{
  "tables": [
    {{
      "table_name": "snake_case",
      "description": "what this table stores",
      "columns": [
        {{
          "name": "column_name",
          "sql_type": "VARCHAR(255)|TEXT|INTEGER|BIGINT|NUMERIC(10,2)|BOOLEAN|TIMESTAMP|DATE|JSONB|UUID",
          "primary_key": false,
          "nullable": true,
          "unique": false,
          "default": null,
          "semantic_type": "same as inferred",
          "index": false
        }}
      ],
      "foreign_keys": [
        {{
          "column": "col_name",
          "references_table": "other_table",
          "references_column": "col_name",
          "on_delete": "CASCADE|SET NULL|RESTRICT"
        }}
      ],
      "indexes": ["col1", "col2"]
    }}
  ],
  "design_notes": "brief explanation of normalization decisions and tradeoffs"
}}

Rules:
- Every table must have a primary key (prefer UUID id column if no natural key exists)
- Add created_at TIMESTAMP column to every table
- Add appropriate indexes on foreign keys and high-cardinality filter columns
- Normalize to 3NF where practical
- Use JSONB for nested/variable structure fields
- Prefer NUMERIC over FLOAT for monetary amounts"""


async def build_data_model(inferred_schema: dict) -> dict:
    prompt = MODELING_PROMPT.format(schema=json.dumps(inferred_schema, indent=2))
    raw = await chat(prompt, system=SYSTEM)
    model = extract_json(raw)
    return model
