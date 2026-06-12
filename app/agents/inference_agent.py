"""
Schema Inference Agent — uses Mistral via Ollama to analyze raw data
and produce a structured schema with semantic field labels.
"""

import json

from app.agents.ollama_client import chat, extract_json
from app.ingestion.base import ParsedData

SYSTEM = """You are a data engineering expert.
When given raw data samples, you infer precise schemas including field names,
data types, semantic meanings, nullability, and relationships between entities.
Always respond with valid JSON only. No explanations outside the JSON."""

INFERENCE_PROMPT = """Analyze the following data sample and infer a complete schema.

Data sample ({row_count} total rows, showing up to {sample_count}):
{sample}

{raw_text_section}

Return a JSON object with this exact structure:
{{
  "source_description": "one sentence describing what this data represents",
  "tables": [
    {{
      "table_name": "snake_case_name",
      "description": "what this table represents",
      "fields": [
        {{
          "name": "field_name",
          "data_type": "string|integer|float|boolean|date|timestamp|json",
          "semantic_type": "e.g. customer_id|email|monetary_amount|event_timestamp|status_code|free_text|...",
          "nullable": true,
          "confidence": 0.95
        }}
      ],
      "relationships": [
        {{
          "from_field": "field_name",
          "to_table": "other_table",
          "to_field": "field_name",
          "relationship_type": "one_to_many|many_to_one|many_to_many"
        }}
      ]
    }}
  ]
}}

Rules:
- Use snake_case for all names
- semantic_type must describe the BUSINESS meaning (not just the data type)
- confidence is 0.0–1.0; use < 0.7 for fields you are uncertain about
- If the data represents a single entity, return one table
- Detect relationships between fields even across a single flat table (e.g. foreign keys)
- For PDF text with no clear tabular structure, model the entities you can extract"""


async def infer_schema(parsed: ParsedData) -> dict:
    sample_str = json.dumps(parsed.rows[:5], indent=2, default=str)
    raw_text_section = ""
    if parsed.raw_text:
        raw_text_section = f"\nExtracted text:\n{parsed.raw_text}"

    prompt = INFERENCE_PROMPT.format(
        row_count=parsed.row_count,
        sample_count=len(parsed.rows),
        sample=sample_str,
        raw_text_section=raw_text_section,
    )

    raw = await chat(prompt, system=SYSTEM)
    schema = extract_json(raw)

    # normalize: ensure top-level keys exist
    if "tables" not in schema:
        schema = {"tables": [schema], "source_description": "", "raw_fields": []}
    if "source_description" not in schema:
        schema["source_description"] = ""

    return schema
