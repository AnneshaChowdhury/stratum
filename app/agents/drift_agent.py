"""
Drift Detection Agent — compares consecutive schema versions,
classifies each change by severity, and asks the LLM to explain
breaking or risky changes in plain English.
"""

import json
from dataclasses import dataclass

from app.agents.ollama_client import chat, extract_json

SEVERITY_MAP = {
    "added": "additive",
    "removed": "breaking",
    "type_changed": "risky",
    "nullable_changed": "risky",
    "semantic_changed": "risky",
}

DRIFT_EXPLAIN_PROMPT = """You are a data engineering expert reviewing schema changes.

Previous schema:
{old_schema}

New schema:
{new_schema}

The following raw changes were detected:
{changes}

For each change, return a JSON array:
[
  {{
    "field_name": "field",
    "change_type": "added|removed|type_changed|nullable_changed|semantic_changed",
    "old_value": "previous value or null",
    "new_value": "new value or null",
    "severity": "additive|risky|breaking",
    "explanation": "one sentence plain-English explanation"
  }}
]

Overall severity rules:
- additive: new nullable fields added — safe to apply automatically
- risky: type changes, nullable→required, semantic label changes — needs human review
- breaking: fields removed, required→nullable on PK, semantic meaning reversal — must block

Return valid JSON array only."""


@dataclass
class DriftResult:
    events: list[dict]
    overall_severity: str   # none | additive | risky | breaking
    summary: str


def _extract_fields(schema: dict) -> dict[str, dict]:
    fields = {}
    for table in schema.get("tables", []):
        for field in table.get("fields", []):
            key = f"{table['table_name']}.{field['name']}"
            fields[key] = field
    return fields


def _classify_overall(events: list[dict]) -> str:
    severity_rank = {"none": 0, "additive": 1, "risky": 2, "breaking": 3}
    max_sev = "none"
    for e in events:
        if severity_rank.get(e["severity"], 0) > severity_rank[max_sev]:
            max_sev = e["severity"]
    return max_sev


async def detect_drift(old_schema: dict, new_schema: dict) -> DriftResult:
    old_fields = _extract_fields(old_schema)
    new_fields = _extract_fields(new_schema)

    raw_changes = []

    for key in old_fields:
        if key not in new_fields:
            raw_changes.append({"field": key, "change": "removed"})
        else:
            old_f = old_fields[key]
            new_f = new_fields[key]
            if old_f.get("data_type") != new_f.get("data_type"):
                raw_changes.append({"field": key, "change": "type_changed",
                                     "old": old_f.get("data_type"), "new": new_f.get("data_type")})
            if old_f.get("nullable") != new_f.get("nullable"):
                raw_changes.append({"field": key, "change": "nullable_changed",
                                     "old": old_f.get("nullable"), "new": new_f.get("nullable")})
            if old_f.get("semantic_type") != new_f.get("semantic_type"):
                raw_changes.append({"field": key, "change": "semantic_changed",
                                     "old": old_f.get("semantic_type"), "new": new_f.get("semantic_type")})

    for key in new_fields:
        if key not in old_fields:
            raw_changes.append({"field": key, "change": "added"})

    if not raw_changes:
        return DriftResult(events=[], overall_severity="none", summary="No schema changes detected.")

    prompt = DRIFT_EXPLAIN_PROMPT.format(
        old_schema=json.dumps(old_schema, indent=2),
        new_schema=json.dumps(new_schema, indent=2),
        changes=json.dumps(raw_changes, indent=2),
    )
    raw = await chat(prompt)
    events = extract_json(raw)
    if isinstance(events, dict):
        events = [events]

    overall = _classify_overall(events)
    breaking = [e for e in events if e["severity"] == "breaking"]
    risky = [e for e in events if e["severity"] == "risky"]
    additive = [e for e in events if e["severity"] == "additive"]

    parts = []
    if breaking:
        parts.append(f"{len(breaking)} breaking change(s): {', '.join(e['field_name'] for e in breaking)}")
    if risky:
        parts.append(f"{len(risky)} risky change(s): {', '.join(e['field_name'] for e in risky)}")
    if additive:
        parts.append(f"{len(additive)} additive change(s): {', '.join(e['field_name'] for e in additive)}")

    summary = "; ".join(parts) or "No significant changes."
    return DriftResult(events=events, overall_severity=overall, summary=summary)
