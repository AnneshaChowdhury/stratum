import json

from app.config import settings
from app.ingestion.base import ParsedData


def _flatten(obj: dict, prefix: str = "", sep: str = ".") -> dict:
    items = {}
    for k, v in obj.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            items.update(_flatten(v, key, sep))
        else:
            items[key] = v
    return items


def parse_json(content: bytes) -> ParsedData:
    data = json.loads(content)

    if isinstance(data, dict):
        rows = [_flatten(data)]
    elif isinstance(data, list):
        rows = [_flatten(r) if isinstance(r, dict) else {"value": r} for r in data]
    else:
        rows = [{"value": data}]

    all_keys: set[str] = set()
    for row in rows:
        all_keys.update(row.keys())

    sample = rows[: settings.max_sample_rows]

    return ParsedData(
        rows=sample,
        all_keys=all_keys,
        row_count=len(rows),
        source_type="json",
    )
