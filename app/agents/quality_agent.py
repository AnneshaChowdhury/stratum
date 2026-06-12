"""
Data Quality Agent — auto-generates quality rules from the inferred schema
and runs them against the actual data sample.
"""

import json
import re
from datetime import datetime

from app.agents.ollama_client import chat, extract_json
from app.ingestion.base import ParsedData

RULE_GEN_PROMPT = """You are a data quality engineer.
Given a schema, generate a set of data quality rules to validate incoming data.

Schema:
{schema}

Return a JSON array of rules:
[
  {{
    "rule_name": "unique_snake_case_name",
    "description": "what this rule checks",
    "field": "field_name or null for table-level",
    "rule_type": "not_null|unique|type_check|range|regex|referential|completeness|freshness",
    "parameters": {{}}
  }}
]

Generate rules for:
1. NOT NULL checks on non-nullable fields
2. Type validation (e.g. email regex, date format)
3. Range checks for numeric fields (no negative amounts, reasonable ranges)
4. Completeness checks (% of non-null values)
5. Uniqueness checks on likely primary key / ID fields
6. Freshness checks on timestamp fields (data not older than 7 days)

Return valid JSON array only."""


async def generate_quality_rules(schema: dict) -> dict:
    prompt = RULE_GEN_PROMPT.format(schema=json.dumps(schema, indent=2))
    raw = await chat(prompt)
    rules = extract_json(raw)
    if isinstance(rules, dict):
        rules = [rules]
    return {"rules": rules}


def run_quality_checks(parsed: ParsedData, rules: dict) -> list[dict]:
    results = []
    rows = parsed.rows

    for rule in rules.get("rules", []):
        rule_name = rule.get("rule_name", "unknown")
        rule_type = rule.get("rule_type", "")
        field = rule.get("field")
        params = rule.get("parameters", {})

        try:
            if rule_type == "not_null" and field:
                nulls = sum(1 for r in rows if r.get(field) is None or r.get(field) == "")
                status = "pass" if nulls == 0 else ("warn" if nulls / len(rows) < 0.1 else "fail")
                results.append(_result(rule_name, status, field, f"{nulls}/{len(rows)} null values",
                                       {"null_count": nulls, "total": len(rows)}))

            elif rule_type == "completeness" and field:
                filled = sum(1 for r in rows if r.get(field) is not None and r.get(field) != "")
                pct = filled / len(rows) if rows else 0
                threshold = params.get("threshold", 0.95)
                status = "pass" if pct >= threshold else ("warn" if pct >= 0.8 else "fail")
                results.append(_result(rule_name, status, field, f"{pct:.1%} complete",
                                       {"completeness": round(pct, 4)}))

            elif rule_type == "unique" and field:
                values = [r.get(field) for r in rows if r.get(field) is not None]
                dupes = len(values) - len(set(str(v) for v in values))
                status = "pass" if dupes == 0 else "fail"
                results.append(_result(rule_name, status, field, f"{dupes} duplicate(s)",
                                       {"duplicate_count": dupes}))

            elif rule_type == "type_check" and field:
                expected = params.get("expected_type", "string")
                failures = []
                for r in rows:
                    v = r.get(field)
                    if v is None:
                        continue
                    if expected == "integer" and not _is_int(v):
                        failures.append(str(v))
                    elif expected == "float" and not _is_float(v):
                        failures.append(str(v))
                    elif expected in ("date", "timestamp") and not _is_datetime(v):
                        failures.append(str(v))
                status = "pass" if not failures else ("warn" if len(failures) < 3 else "fail")
                results.append(_result(rule_name, status, field, f"{len(failures)} type mismatch(es)",
                                       {"failures": failures[:5]}))

            elif rule_type == "regex" and field:
                pattern = params.get("pattern", "")
                if pattern:
                    failures = [str(r.get(field)) for r in rows
                                if r.get(field) and not re.match(pattern, str(r.get(field)))]
                    status = "pass" if not failures else "fail"
                    results.append(_result(rule_name, status, field, f"{len(failures)} pattern mismatch(es)",
                                           {"failures": failures[:5]}))

            elif rule_type == "range" and field:
                mn, mx = params.get("min"), params.get("max")
                out_of_range = []
                for r in rows:
                    v = r.get(field)
                    if v is None:
                        continue
                    try:
                        fv = float(v)
                        if (mn is not None and fv < mn) or (mx is not None and fv > mx):
                            out_of_range.append(fv)
                    except (ValueError, TypeError):
                        pass
                status = "pass" if not out_of_range else "fail"
                results.append(_result(rule_name, status, field, f"{len(out_of_range)} out-of-range value(s)",
                                       {"violations": out_of_range[:5]}))

        except Exception as exc:
            results.append(_result(rule_name, "warn", field, f"Rule check error: {exc}", {}))

    return results


def _result(rule_name: str, status: str, field: str | None, message: str, details: dict) -> dict:
    return {"rule_name": rule_name, "status": status, "field": field,
            "message": message, "details": details}


def _is_int(v) -> bool:
    try:
        int(str(v))
        return True
    except (ValueError, TypeError):
        return False


def _is_float(v) -> bool:
    try:
        float(str(v))
        return True
    except (ValueError, TypeError):
        return False


def _is_datetime(v) -> bool:
    formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%m/%d/%Y"]
    for fmt in formats:
        try:
            datetime.strptime(str(v), fmt)
            return True
        except (ValueError, TypeError):
            pass
    return False
