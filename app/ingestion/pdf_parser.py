import io
import re

import pdfplumber

from app.config import settings
from app.ingestion.base import ParsedData


def parse_pdf(content: bytes) -> ParsedData:
    rows: list[dict] = []
    text_blocks: list[str] = []

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        pages = pdf.pages[: settings.max_pdf_pages]
        for page in pages:
            # extract tables if present
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue
                headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(table[0])]
                for data_row in table[1:]:
                    row = {headers[i]: (cell.strip() if cell else None) for i, cell in enumerate(data_row)}
                    rows.append(row)

            text = page.extract_text() or ""
            if text:
                text_blocks.append(text)

    full_text = "\n".join(text_blocks)

    # if no tables found, treat each non-empty line as a text chunk for the LLM to reason about
    if not rows:
        lines = [l.strip() for l in full_text.splitlines() if l.strip()]
        rows = [{"text": line} for line in lines[: settings.max_sample_rows]]

    all_keys: set[str] = set()
    for row in rows:
        all_keys.update(row.keys())

    return ParsedData(
        rows=rows[: settings.max_sample_rows],
        all_keys=all_keys,
        row_count=len(rows),
        source_type="pdf",
        raw_text=full_text[:4000],  # cap text sent to LLM
    )
