import io

import pandas as pd

from app.config import settings
from app.ingestion.base import ParsedData


def parse_csv(content: bytes) -> ParsedData:
    df = pd.read_csv(io.BytesIO(content))
    df = df.where(pd.notna(df), None)

    sample = df.head(settings.max_sample_rows)
    rows = sample.to_dict(orient="records")
    all_keys = set(df.columns.tolist())

    return ParsedData(
        rows=rows,
        all_keys=all_keys,
        row_count=len(df),
        source_type="csv",
    )
