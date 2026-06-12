# Stratum

> **Adaptive AI agent that ingests unstructured data from any source, iteratively infers schemas, detects drift, and runs auto-generated data quality checks — all without a human writing a single schema.**

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?style=flat-square&logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![LLM](https://img.shields.io/badge/LLM-Llama%203.3%2070B-FF6B35?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## What is Stratum?

Most data pipelines break the moment the data changes. Schema drift silently corrupts models. Quality rules are written by hand, months after the data arrived. And unstructured data — PDFs, JSON blobs, event streams — never makes it into clean models at all.

**Stratum solves this.** It's an AI agent that:

1. **Ingests** raw data from any source — CSV, JSON, PDF, Kafka (coming soon), REST APIs
2. **Infers** a full schema with semantic field labels, types, and relationships — powered by LLM
3. **Models** your data into a normalized relational structure with DDL-ready output
4. **Detects drift** when your data changes — classifying every change as safe, risky, or breaking
5. **Generates and runs** data quality rules automatically on every ingestion

No YAML. No manual schema definitions. No data contracts written by hand. Stratum figures it out — and keeps figuring it out as your data evolves.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                          │
│          CSV  ·  JSON  ·  PDF  ·  Kafka (soon)  ·  APIs        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SCHEMA INFERENCE AGENT                        │
│   LLM analyzes raw data → infers field names, types,           │
│   semantic meanings (email, monetary_amount, customer_id…)      │
│   and detects relationships between entities                    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ITERATIVE MODELING LOOP                        │
│                                                                 │
│   New data arrives → diff against current schema               │
│        ↓                                                        │
│   Drift detected? → classify: additive / risky / breaking      │
│        ↓                                                        │
│   Revise model → run quality gates → stabilize                 │
│        ↓                                                        │
│   Repeat recursively until convergence                         │
└──────────────────────────────┬──────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
       ┌───────────┐  ┌──────────────┐  ┌───────────────┐
       │  Data     │  │   Drift      │  │   Quality     │
       │  Model    │  │   Events     │  │   Results     │
       │  + DDL    │  │   + Alerts   │  │   + Rules     │
       └───────────┘  └──────────────┘  └───────────────┘
                               │
                               ▼
                    ┌──────────────────┐
                    │   PostgreSQL     │
                    │  (versioned      │
                    │   schema store)  │
                    └──────────────────┘
```

---

## Key Features

### Schema Inference from Unstructured Data
Upload a raw CSV, a nested JSON, or a PDF with tables — Stratum extracts entities, infers data types, and assigns semantic labels like `customer_id`, `monetary_amount`, `event_timestamp` — not just `string` or `integer`.

### Iterative & Recursive Modeling
Every time new data arrives from the same source, Stratum compares it to the previous schema version and evolves the model. It tracks every version, so you have a full history of how your data changed.

### Drift Severity Classification
Not all schema drift is equal. Stratum classifies every change:

| Severity | What happened | Action |
|---|---|---|
| **Additive** | New nullable field added | Auto-apply |
| **Risky** | Type change, field rename, nullability shift | Flag for review |
| **Breaking** | Field removed, semantic meaning reversed | Block + alert |

### Auto-Generated Data Quality Rules
From the inferred schema, Stratum generates and runs quality rules automatically:
- Null checks on required fields
- Type validation (email regex, date formats, numeric ranges)
- Completeness percentages
- Uniqueness checks on ID fields
- Range checks on numeric values

### DDL Export
Get production-ready `CREATE TABLE` SQL from the latest inferred model — with primary keys, foreign keys, indexes, and the right column types.

### Semantic Field Understanding
Fields aren't just `type: string`. Stratum understands that `email` means an email address, `customer_id` is a foreign key candidate, and `monthly_spend` should be `NUMERIC(10,2)` — not `FLOAT`.

---

## Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI + Uvicorn |
| Database | PostgreSQL 16 + SQLAlchemy 2.0 async |
| LLM | Llama 3.3 70B via Groq (free tier) |
| Ingestion | pandas, pdfplumber |
| Validation | Pydantic v2 |
| Packaging | Hatchling |

---

## Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL 16
- A free [Groq API key](https://console.groq.com) (no credit card required)

### 1. Clone & install

```bash
git clone git@github.com:AnneshaChowdhury/stratum.git
cd stratum
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env`:

```env
DATABASE_URL=postgresql+asyncpg://stratum@localhost:5432/stratum
GROQ_API_KEY=gsk_your_key_from_console.groq.com
GROQ_MODEL=llama-3.3-70b-versatile
```

### 3. Set up the database

```bash
createuser -s stratum
createdb -U stratum stratum
```

### 4. Run

```bash
uvicorn app.main:app --reload --port 8100
```

API is live at `http://localhost:8100` — interactive docs at `http://localhost:8100/docs`

---

## API Reference

### `POST /api/v1/ingest`
Upload any file and run the full agent pipeline.

```bash
curl -X POST http://localhost:8100/api/v1/ingest \
  -F "file=@your_data.csv" \
  -F "source_name=customers"
```

**Response includes:**
- Inferred schema with semantic field labels
- Normalized data model
- Drift classification vs previous version
- Auto-generated quality rules + check results

---

### `GET /api/v1/sources`
List all ingested data sources with their full schema version history.

---

### `GET /api/v1/sources/{id}/ddl`
Get `CREATE TABLE` SQL from the latest data model.

```sql
CREATE TABLE IF NOT EXISTS customers (
  id UUID PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  monthly_spend NUMERIC(10,2),
  signup_date DATE,
  created_at TIMESTAMP
);
```

---

### `GET /api/v1/sources/{id}/quality`
Get all quality check results for a source across ingestions.

---

## How the Agent Pipeline Works

When you `POST /ingest`:

```
1. Parse file         → extract rows + fields
2. Inference Agent    → LLM infers schema with semantic types
3. Modeling Agent     → LLM builds normalized relational model
4. Quality Agent      → LLM generates rules → rules run on real data
5. Drift Agent        → compare to previous version, classify changes
6. Persist            → store schema version, drift events, quality results
7. Return             → full response with everything above
```

All four agents run sequentially with a single file upload. On the second upload of the same source, drift detection kicks in automatically.

---

## Roadmap

- [ ] Kafka topic ingestion (real-time streaming)
- [ ] REST API poller (ingest from any endpoint on a schedule)
- [ ] Downstream impact analysis (what breaks when schema changes)
- [ ] Natural language model editing ("make this table track history")
- [ ] Data contract auto-generation + enforcement
- [ ] PII auto-detection and masking
- [ ] Confidence-scored active learning (flag low-confidence fields for human review)
- [ ] Synthetic data generation for schema testing
- [ ] Multi-modal ingestion (images via OCR, audio transcripts)
- [ ] Web UI dashboard

---

## Docker

```bash
docker compose up
```

Spins up PostgreSQL + the API. Set `GROQ_API_KEY` in your environment first.

---

## License

MIT — build whatever you want with it.

---

<p align="center">
  Built with intention. Data has layers — so does Stratum.
</p>
