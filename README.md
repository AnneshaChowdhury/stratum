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

1. **Ingests** raw data from any source — CSV, JSON, PDF, Kafka streams, REST APIs
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
│          CSV  ·  JSON  ·  PDF  ·  Kafka  ·  APIs               │
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

### One-command quickstart (recommended)

The fastest way to run Stratum. All you need is Docker and a free Groq API key.

**1. Get a free Groq API key** at [console.groq.com](https://console.groq.com) — no credit card required.

**2. Clone and start:**

```bash
git clone git@github.com:AnneshaChowdhury/stratum.git
cd stratum
GROQ_API_KEY=gsk_... docker compose up
```

That's it. Docker Compose spins up PostgreSQL, Kafka, and the API, then automatically runs a seed job that posts demo data through the full agent pipeline and prints the results:

```
╭──────────────────────────────────────────────────────╮
│  Stratum                                             │
│  AI-powered schema inference, drift detection        │
│  & data quality                                      │
╰──────────────────────────────────────────────────────╯

API ready — model: llama-3.3-70b-versatile, backend: groq
Ingesting customers.csv...
  ✓ Done — schema version 1
Ingesting orders.json...
  ✓ Done — schema version 1

┌─ customers ────────────────────────────────────────────┐
│ Customer subscription and contact records              │
│                                                        │
│ Schema — 1 table, 9 fields inferred                   │
│  Table      Fields  Description                        │
│  customers  9       Customer subscription records      │
│                                                        │
│ Quality — 6 pass  0 warn  0 fail                      │
└────────────────────────────────────────────────────────┘

╭──────────────────────────────────╮
│  Stratum is ready!               │
│                                  │
│  API:   http://api:8000          │
│  Docs:  http://api:8000/docs     │
╰──────────────────────────────────╯
```

Open **http://localhost:8000/docs** to explore the API.

---

### Local development (without Docker)

#### Prerequisites

- Python 3.11+
- PostgreSQL 16 running locally
- A free [Groq API key](https://console.groq.com)

#### Setup

```bash
git clone git@github.com:AnneshaChowdhury/stratum.git
cd stratum
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env   # then add your GROQ_API_KEY
```

Create the database:

```bash
createuser -s stratum
createdb -U stratum stratum
```

Run:

```bash
uvicorn app.main:app --reload --port 8000
```

API is live at `http://localhost:8000` — interactive docs at `http://localhost:8000/docs`

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

## Kafka ingestion

Register a topic and Stratum starts consuming it in real-time — running the full agent pipeline on each batch of messages:

```bash
curl -X POST http://localhost:8000/api/v1/kafka/topics \
  -H "Content-Type: application/json" \
  -d '{"topic": "events", "bootstrap_servers": "localhost:9092"}'
```

Stratum batches messages (50 messages or 5 seconds, whichever comes first), runs inference → modeling → quality → drift on each batch, and persists versioned schemas as the message shape evolves.

Active topics survive restarts — they're automatically resumed when the API starts.

---

## License

MIT — build whatever you want with it.

---

<p align="center">
  Built with intention. Data has layers — so does Stratum.
</p>
