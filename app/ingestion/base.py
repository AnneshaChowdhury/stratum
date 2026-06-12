from dataclasses import dataclass


@dataclass
class ParsedData:
    rows: list[dict]        # sample rows for LLM inference
    all_keys: set[str]      # every field key seen across all rows
    row_count: int
    source_type: str
    raw_text: str = ""      # for PDF: extracted text
