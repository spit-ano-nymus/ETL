
# ETL Studio

A YAML-driven ETL pipeline for streaming CSV and Excel files into SQL Server or AWS S3, with an optional Streamlit web interface for non-technical users.

---

## Features

- **Chunked streaming** — processes files of any size (1.5 GB+) with constant RAM usage (one 10 000-row chunk at a time)
- **Four load modes** — `replace`, `append`, `upsert`, `skip_existing`
- **Transform pipeline** — cleaning, validation, and parsing steps via a plug-in registry
- **Audit log** — every run writes a row to `etl_audit_log` in SQL Server
- **Web UI** — Streamlit front-end for point-and-click ETL without touching YAML or a terminal
- **S3 destination** — writes Parquet chunks to AWS S3 via boto3

---

## Project Structure

```
ETL/
├── main.py                         # CLI entry point
├── requirements.txt                # Core dependencies
├── requirements-web.txt            # Web UI dependencies
├── .streamlit/
│   └── config.toml                 # Streamlit server settings
├── core/
│   ├── consts.py                   # Shared constants
│   ├── pipeline_runner.py          # Orchestrates extract → transform → load
│   └── registry.py                 # Transform plug-in registry (@register decorator)
├── extract/
│   └── csv_reader.py               # stream_csv() — chunked CSV generator
├── transform/
│   ├── cleaners.py                 # trim_whitespace, normalize_nulls
│   ├── validators.py               # validate_required, validate_no_duplicates
│   └── parsers/
│       ├── date_parser.py
│       ├── email_parser.py
│       ├── phone_parser.py
│       └── numeric_parser.py
├── db/
│   ├── engine.py                   # SQLAlchemy engine factory
│   └── loader.py                   # bulk_load() — SQL Server writer
├── utils/
│   ├── file_utils.py               # resolve_path(), assert_readable()
│   └── logging_utils.py
└── web/
    ├── app.py                      # Streamlit entry point (4-step UI)
    ├── config.py                   # Session state initialisation
    ├── components/
    │   ├── file_input.py           # Path / upload tabs
    │   ├── destination_form.py     # SQL Server / S3 credential forms
    │   ├── action_selector.py      # Action checkboxes from registry
    │   ├── batch_queue.py          # Queued jobs table
    │   ├── preview_table.py        # Data preview + column stats
    │   ├── progress_tracker.py     # Live progress bars
    │   └── sql_window.py           # SQL scratch pad (draft only)
    ├── services/
    │   ├── file_service.py         # Path validation, sampling, Excel chunking
    │   ├── pipeline_service.py     # Background worker thread
    │   ├── destination_service.py  # Writer factory, table existence check
    │   ├── transform_service.py    # apply_selected_transforms() via registry
    │   └── progress_service.py     # Thread-safe ProgressQueue
    └── s3/
        ├── engine.py               # boto3 client factory (lru_cache)
        └── loader.py               # s3_load() — writes Parquet chunks
```

---

## Installation

### Core (CLI only)

```bash
pip install -r requirements.txt
```

### Web UI

```bash
pip install -r requirements-web.txt
```

**Requirements:** Python 3.11+, an ODBC driver for SQL Server (e.g. [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)).

---

## CLI Usage

### Pipeline YAML

Create a config file describing your pipeline:

```yaml
pipeline:
  name: my_pipeline

source:
  path: C:/data/customers.csv
  separator: ","
  encoding: utf-8-sig
  chunk_size: 10000

transform:
  steps:
    - trim_whitespace
    - normalize_nulls
    - name: validate_required
      params:
        columns: [id, email]
    - name: validate_no_duplicates
      params:
        columns: [id]

destination:
  table: customers
  schema: dbo
  load_mode: upsert       # replace | append | upsert | skip_existing
  primary_keys: [id]
  quarantine_table: customers_quarantine   # optional: rows failing validation land here

audit:
  enabled: true
  schema: dbo
```

### Run

```bash
# Run with a config file
python main.py --pipeline config/pipeline.yaml

# Override the source file at runtime
python main.py --pipeline config/pipeline.yaml --file C:/data/customers_jan.csv

# Increase log verbosity
python main.py --pipeline config/pipeline.yaml --log-level DEBUG
```

**Exit codes:** `0` = success, `1` = failure.

---

## Web UI Usage

```bash
streamlit run web/app.py
# Opens at http://localhost:8501
```

The UI walks through four steps:

| Step | What happens |
|------|-------------|
| **1 — Input & Destination** | Choose a file (server path or upload) and configure the SQL Server or S3 destination. Add multiple files to a batch queue. |
| **2 — Actions & Batch Review** | Select transform actions (cleaning, validation, parsing, mining). Review the queue. |
| **3 — Review** | Preview the first 1 000 rows. If the target table already exists, choose a load mode. View column statistics. Draft a SQL query (not executed). |
| **4 — Upload & Progress** | Jobs run sequentially. Live progress bars update every 300 ms. Download the audit log CSV when done. |

---

## Step 2 — Actions

In Step 2 you choose which data-quality actions to run on every file in the batch.
Two actions are enabled by default; the other two are opt-in because they need you to
specify which columns to target.

---

### Cleaning ✅ (on by default)

Fixes the most common raw-data noise: extra spaces and empty/null values.

**What it does**
- `trim_whitespace` — strips leading and trailing spaces from every text column
- `normalize_nulls` — converts empty strings and `"nan"` text into a real SQL `NULL`

**When to use:** Almost always. Leave this on unless you specifically want to preserve
leading spaces or treat empty strings as non-null.

**Example**

| Column | Before | After |
|--------|--------|-------|
| `name` | `"  Alice  "` | `"Alice"` |
| `city` | `"  "` | `NULL` |
| `code` | `"nan"` | `NULL` |

---

### Validation (opt-in)

Guards data integrity by quarantining rows that break your rules.
Quarantined rows are written to a separate `_quarantine` table instead of your target table,
so no bad data is silently swallowed.

**What it does**
- `validate_required` — quarantines rows where a required column is null or blank
- `validate_no_duplicates` — quarantines duplicate rows on the columns you specify (keeps the first occurrence)

**When to use:** Enable this when your database has NOT NULL constraints or a primary key
that must be unique, and you want the pipeline to flag violations rather than fail or silently insert bad data.

**Example — required field check on `id` and `email`**

| id | email | Outcome |
|----|-------|---------|
| 1 | alice@example.com | ✅ loaded |
| 2 | _(empty)_ | ❌ quarantined — email missing |
| _(empty)_ | bob@example.com | ❌ quarantined — id missing |

**Example — duplicate check on `id`**

| id | name | Outcome |
|----|------|---------|
| 1 | Alice | ✅ loaded (first occurrence) |
| 1 | Alice duplicate | ❌ quarantined — duplicate id |
| 2 | Bob | ✅ loaded |

---

### Parsing (opt-in)

Standardises messy values in specific columns so they arrive in the database in a
consistent, queryable format.

**What it does — one parser per column type**

| Parser | What it standardises | Invalid values become |
|--------|---------------------|-----------------------|
| `date_parser` | Parses text dates in any common format into a real date | `NULL` |
| `email_parser` | Lowercases and validates email format | `NULL` |
| `phone_parser` | Strips non-digit characters (spaces, dashes, parentheses) | digits only, or `NULL` |
| `numeric_parser` | Removes thousands separators (`,`) and converts to a number | `NULL` |

**When to use:** Enable this when your source file has inconsistent formatting in date,
email, phone, or number columns and you want clean, typed values in the database.

**Example**

| Column | Before | After |
|--------|--------|-------|
| `signup_date` | `"03/15/2024"` | `2024-03-15` |
| `email` | `" Alice@EXAMPLE.COM "` | `"alice@example.com"` |
| `phone` | `"(212) 555-1234"` | `"2125551234"` |
| `revenue` | `"1,250,000.50"` | `1250000.5` |
| `email` | `"not-an-email"` | `NULL` |

---

### Mining ✅ (on by default)

Profiles every column in your file and shows the results in Step 3 — Review.
No data is modified.

**What it shows**

| Stat | Meaning |
|------|---------|
| Null % | Percentage of rows where this column is empty |
| Unique count | Number of distinct values |
| Min / Max | Smallest and largest value |
| Mean | Average (numeric columns only) |

**When to use:** Always useful as a sanity check before loading. Disable it only if the
file is very large and you want to skip the profiling overhead.

**Example output for a `revenue` column**

| Null % | Unique | Min | Max | Mean |
|--------|--------|-----|-----|------|
| 2.1 % | 4 820 | 0.0 | 9 850 000.0 | 47 320.5 |

---

## Transform Registry

Transforms are registered with the `@register` decorator and resolved by name in YAML configs or the web UI.

### Built-in transforms

| Name | Description |
|------|-------------|
| `trim_whitespace` | Strip leading/trailing whitespace from all string columns |
| `normalize_nulls` | Replace empty strings and `"nan"` with `None` (SQL NULL) |
| `validate_required` | Quarantine rows with null/blank values in specified columns |
| `validate_no_duplicates` | Quarantine duplicate rows on specified columns |
| `date_parser` | Parse date columns; unparseable values become `NaT` |
| `email_parser` | Lowercase + validate email format; invalid values become `None` |
| `phone_parser` | Strip non-digits (or E.164 format if `phonenumbers` is installed) |
| `numeric_parser` | Remove thousands separator, convert to float |

### Adding a new action group (like "Cleaning")

An **action group** is a named checkbox in Step 2 that runs one or more transforms together.
To add your own group end-to-end, edit **three files**:

---

#### Step 1 — Write the transform function(s)

Create a file in `transform/` (or add to an existing one):

```python
# transform/formatters.py
from core.registry import register
import pandas as pd

@register("uppercase_names")
def uppercase_names(df: pd.DataFrame, columns: list[str], **kwargs) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.upper()
    return df
```

Rules:
- The first argument must be `df: pd.DataFrame`
- Always return a DataFrame (or a `ValidationResult` if you want rows quarantined — see `transform/validators.py`)
- Accept `**kwargs` so the registry can pass extra parameters safely

---

#### Step 2 — Register the module and add it to an action group

**File: `web/services/transform_service.py`**

```python
# 1. Import your new module so @register runs at startup
import transform.formatters  # noqa: F401   ← add this line near the top

# 2. Add your new group (or add steps to an existing group)
ACTION_STEPS: dict[str, list[str]] = {
    "cleaning":    ["trim_whitespace", "normalize_nulls"],
    "validation":  ["validate_required", "validate_no_duplicates"],
    "parsing":     ["date_parser", "email_parser", "phone_parser", "numeric_parser"],
    "formatting":  ["uppercase_names"],   # ← new group
}
```

---

#### Step 3 — Add the checkbox label and default

**File: `web/components/action_selector.py`**

```python
_ACTION_LABELS = {
    "cleaning":   "Cleaning  —  trim whitespace, normalize nulls",
    "validation": "Validation  —  required fields, no duplicates",
    "parsing":    "Parsing  —  date, email, phone, numeric parsers",
    "mining":     "Mining  —  column statistics (null %, unique, min/max/mean)",
    "formatting": "Formatting  —  uppercase names",   # ← add this
}

_ACTION_DEFAULTS = {
    "cleaning":   True,
    "validation": False,
    "parsing":    False,
    "mining":     True,
    "formatting": False,   # ← add this (True = checked by default)
}
```

---

#### No YAML changes needed for the web UI

If you also use the CLI (`main.py` + a YAML pipeline config), reference your step by its registered name:

```yaml
transform:
  steps:
    - name: uppercase_names
      params:
        columns: [first_name, last_name]
```

---

### Adding a custom transform

```python
# my_transforms.py
from core.registry import register
import pandas as pd

@register("uppercase_names")
def uppercase_names(df: pd.DataFrame, columns: list[str], **kwargs) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.upper()
    return df
```

Import the module before calling `run_pipeline()` and reference it in your YAML:

```yaml
transform:
  steps:
    - name: uppercase_names
      params:
        columns: [first_name, last_name]
```

---

## Load Modes

| Mode | Behaviour |
|------|-----------|
| `replace` | Drop and recreate the table on the first chunk; append subsequent chunks |
| `append` | Always insert; create the table if it does not exist |
| `upsert` | Insert new rows; UPDATE existing rows matched on `primary_keys` |
| `skip_existing` | Insert only rows whose `primary_keys` are not already in the table |

---

## Database Engine

`db/engine.py` exposes `get_engine(connection_string=None)`:

- **CLI path** — calls your infra library (adapt the `MSSQLClient` import).
- **Web UI path** — pass a SQLAlchemy URL directly:

```python
from db.engine import get_engine

engine = get_engine(
    "mssql+pyodbc:///?odbc_connect=driver=ODBC+Driver+17+for+SQL+Server;"
    "server=localhost;database=MyDB;uid=sa;pwd=secret"
)
```

---

## Audit Log

When `audit.enabled: true`, each run appends one row to `etl_audit_log` (auto-created):

| Column | Description |
|--------|-------------|
| `run_id` | UUID for the run |
| `pipeline` | Pipeline name from YAML |
| `source_file` | Resolved source path |
| `started_at` / `finished_at` | UTC timestamps |
| `rows_read` / `rows_inserted` / `rows_updated` / `rows_skipped` / `rows_errored` | Row counts |
| `status` | `"success"` or `"failure"` |
| `error_detail` | Exception message on failure |

The web UI always offers an audit log CSV download after a run completes.

---

## S3 Destination

Configure in the web UI or build the destination dict directly:

```python
{
    "type": "s3",
    "access_key": "AKIA...",
    "secret_key": "...",
    "region": "us-east-1",
    "bucket": "my-data-lake",
    "key_prefix": "etl-output/customers",
}
```

Each chunk is written as a Parquet file: `etl-output/customers/part-00000.parquet`, `part-00001.parquet`, …

`load_mode="replace"` deletes all objects under the prefix before writing the first chunk.

---

## Large File Handling

| Input method | RAM behaviour |
|---|---|
| Server path (CSV) | `stream_csv()` yields one chunk at a time — full file never in RAM |
| Server path (Excel) | `_stream_excel()` reads with `skiprows`/`nrows` — full file never in RAM |
| Upload | Streamed to `/tmp/etl_uploads/<session_id>/`, then treated as a server path |
| Preview | Reads only the first 1 000 rows |

Peak RAM usage = one chunk (default 10 000 rows) regardless of file size.
