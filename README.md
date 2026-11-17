
# ETL Studio

A YAML-driven ETL pipeline for streaming CSV and Excel files into SQL Server, PostgreSQL, or AWS S3, with an optional Streamlit web interface for non-technical users.

---

## One-click pipeline (staging → public → analyst)

One command per table moves a full supplier fetch (Trino or CSV) through three schemas, each with its own job:

| Schema | What it holds | Who uses it |
|---|---|---|
| `staging` | The fetch exactly as it arrived, all text, no key. Rebuilt every run. | nobody (technical) |
| `public` | Clean raw: no duplicates, no supplier extra columns, **original column names**, all text. The data team's hermetic working area. | data team |
| `analyst` (any schema you set per table) | Hebrew column names, real types (dates, numbers), unwanted rows removed. Snapshot tables keep every version. | analysts |

```
supplier ──► staging ──► public ──► analyst
            step 1      step 2     step 3
```

### Where things live

| What | Where | Edited by |
|---|---|---|
| Table rules: snapshot or not, record key, fetch-time column, supplier columns to drop, target schema/table, rows to exclude | Postgres: `meta.etl_tables` | you, in pgAdmin (SQL `INSERT` / `UPDATE`) |
| The mapper: original name → Hebrew name, type, date format, reason | Postgres: `meta.etl_column_map` | you, in pgAdmin |
| Where to read from and how to connect | `config/tables/<table>.yaml` (one file per table) | you, in PyCharm |
| The logic | `sql/*.sql` and the Python code | git |

The mapper in Postgres is the **only** place renames are defined. (The old `consts/column_aliases.py` and the Streamlit rename screen were removed.)

### What a run does

| Step | What happens | Code |
|---|---|---|
| 0 · setup | Runs every file in `sql/` that is new or changed since it last ran, and records it (with its full content) in `meta.schema_migrations`. Nobody pastes SQL into pgAdmin. | `db/migrations.py` |
| 1 · fetch | Streams the source into `staging.<raw_table>` with `COPY`. All values text; `NULL` and empty string stay different. | `core/table_pipeline.py` |
| 2 · raw | `meta.load_raw(table_key)`: keeps this run's staging copy (if `keep_staging_days` is set), ignores the supplier's extra columns, runs the table's `pre_table_sql`, applies each column's `pre_expr`, hashes every remaining column except the fetch time, and inserts only what is new (see below). Creates the raw table on the first run and adds new source columns automatically. | `sql/002_load_raw.sql` |
| 3 · final | `meta.build_final(table_key)`: rebuilds the analyst table from raw: renames, cleans with `transform_expr` and converts by the mapper, drops rows matching `exclude_where`. A value that cannot be converted (e.g. `31/02/2020` as a date) becomes NULL and is counted, it does not fail the run. Then any `py_func` column is run through its Python function. | `sql/003_build_final.sql`, `transform/column_functions.py` |

Steps 2 and 3 run in **one transaction**: if step 3 fails, step 2 is undone too, so `public` and `analyst` never disagree. Every run is logged in `meta.etl_runs`.

### The two table types

| | `snapshot` | `insert_only` |
|---|---|---|
| Supplier sends | the full table every time, including unchanged records | rows that never change, some already sent before |
| Step 2 inserts into `public` | a record that is new, or whose values differ from **its latest version** (a record that changes back to an old value also gets a new version) | a row only if no identical row exists; it keeps the fetch time of its **first** appearance |
| Step 3 in `analyst` | every version, plus `מספר_גרסה` (1 = first) and `גרסה_אחרונה` (true on the latest) | every row |
| Needs `business_key` | yes | no — "duplicate" means all columns equal (except fetch time) |

Raw tables get two technical columns: `_row_hash` (md5 of the compared columns) and `_raw_id` (insertion order; also the raw table's primary key). Original columns are never changed.

### Where your own logic goes

Two points in the run, two kinds of logic. Everything is configured per table, nothing is hard-coded.

| | staging → public (`pre`) | public → analyst (`post`) |
|---|---|---|
| **One column, SQL** | `pre_expr` in the mapper | `transform_expr` in the mapper |
| **Whole table, SQL** | `pre_table_sql` in `meta.etl_tables` | `exclude_where` in `meta.etl_tables` |
| **One column, Python** | — | `py_func` in the mapper |

**Which point to use.** `public` is what you can always go back to, so prefer `post`: change the rule, run `--final-only`, and the analyst table is rebuilt from all of history without touching the source. A `pre` rule changes what `public` stores, and `public` is the only record of how a row looked in earlier fetches — the supplier will not send those again. So any table with `pre_expr` or `pre_table_sql` should also set `keep_staging_days`, which leaves each run's untouched fetch in `staging` as `<table>_<yyyymmdd_hhmmss>` and drops copies past that age (tracked in `meta.staging_snapshots`). A run that changes values before `public` without it logs a warning.

**Where the SQL itself is written.** Three levels:

1. A one-line expression used by one column of one table → inline in `pre_expr` / `transform_expr`, in that table's `_setup.sql`.
2. A function used by one table → `CREATE OR REPLACE FUNCTION` at the top of that table's `_setup.sql`, then call it by name.
3. A function used by several tables → `sql/004_clean_functions.sql`, which is applied automatically.

Defining a function never applies it: a column is cleaned only where its `pre_expr` / `transform_expr` names the function. The only logic that runs on every table without being named is the fixed part of step 2 — dropping `drop_cols` and dropping duplicates.

**Python functions** are for what SQL cannot do, above all calling an outside service. Write one in `transform/column_functions.py`, name it in the column's `py_func`, and the runner passes it the column's distinct values after the final table is built, in the same transaction. Every result is cached in `meta.py_func_cache`, so a value is sent to the service once and not again on later runs. `transform/column_functions.py` ships `strip_nikud`, `normalize_space_py`, and `translate_he` with one function left for you to wire to your translation engine.

```sql
-- clean an id on the way into public, and keep a safety copy of each fetch
UPDATE meta.etl_tables     SET keep_staging_days = 90 WHERE table_key = 'customer';
UPDATE meta.etl_column_map SET pre_expr = 'meta.digits_only({col})'
 WHERE table_key = 'customer' AND source_col = 'person_id';

-- drop junk rows from the fetch before it reaches public
UPDATE meta.etl_tables
   SET pre_table_sql = ARRAY[$$DELETE FROM {table} WHERE cust_no LIKE 'TEST%'$$]
 WHERE table_key = 'customer';

-- translate a text column on the way to the analysts
UPDATE meta.etl_column_map SET py_func = 'translate_he'
 WHERE table_key = 'customer' AND source_col = 'notes';
```

`{col}` is the column, `{table}` is the staging table; both are required in those fields and the run stops with a clear message if they are missing or the SQL is invalid.

### Step by step: adding a table, from nothing to a running pipeline

Written so that someone who has never seen this project can follow it. Every command is run from the project folder (the one with `main.py`), in a terminal — PyCharm's terminal is fine.

#### Once per machine

**1. Install the Python packages.**
```
pip install -r requirements.txt
```

**2. Put the database password in an environment variable.** It is never written in a file.
```powershell
# Windows PowerShell — do this again in every new terminal window
$env:PG_PASSWORD = "your-postgres-password"
```
```bash
# Linux / macOS
export PG_PASSWORD='your-postgres-password'
```

#### Once per table

**3. Create the table's YAML** at `config/tables/<table>.yaml`. Copy `config/tables/example_snapshot.yaml` (Trino) or `example_weekly_file.yaml` (CSV) and change the values. This file says only **where to read from** and **how to connect** — nothing about the table's contents.

```yaml
pipeline:
  table_key: orders            # the name you will use in every command

source:
  type: trino
  host: trino.internal
  port: 8080
  user: etl_user
  password_env: TRINO_PASSWORD # optional, the name of an environment variable
  catalog: hive
  schema: sales
  table: orders
  chunk_size: 10000

postgres:
  host: pg.internal
  port: 5432
  database: data
  user: etl_user
  password_env: PG_PASSWORD
```

**4. Answer five questions about the table.** This is the only part that needs thinking:

1. Can a row change later? Yes → `snapshot` (every version is kept). No → `insert_only`.
2. Which column(s) identify a record? (only needed for `snapshot`)
3. Which column is the supplier's "when I pulled this" timestamp?
4. Which columns does the supplier add that must not be stored?
5. Which rows should the analysts not see?

**5. Create the table's config file** at `config/tables/<table>_setup.sql`, next to the YAML. Copy `tests/local/setup_customer.sql` and edit it. It is plain SQL: one row describing the table, one row per column. It starts with a `DELETE`, so it is safe to edit and re-apply as often as you like.

**6. Apply it.** This also creates the `meta` tables the first time it runs against a database:
```
python main.py --pipeline config/tables/orders.yaml --sql config/tables/orders_setup.sql
```
It prints back what it stored. Check the names and types before going on.

**7. Run it.**
```
python main.py --pipeline config/tables/orders.yaml
```
The output tells you how many rows arrived, how many were new, and how many reached the analysts.

**8. Look at the result.**
```
python main.py --pipeline config/tables/orders.yaml --sql tests/local/show_objects.sql
```

**9. Schedule it, if it should run by itself.** One cron line, the same command. cron does not inherit your shell, so set the password in the line:
```
0 3 * * 0 cd /opt/etl && PG_PASSWORD=secret .venv/bin/python main.py --pipeline config/tables/orders.yaml
```

#### When something changes later

| What changed | What to do |
|---|---|
| A column name, a type, a cleaning rule, which rows are excluded | Edit `<table>_setup.sql`, re-apply it with `--sql`, then `python main.py --pipeline ... --final-only` |
| The supplier added a column | Nothing: it is added to `public` automatically. Add a mapper row for it if the analysts should see it renamed. |
| A new table | Steps 3–7 again |
| Pause a table | `UPDATE meta.etl_tables SET is_active = false WHERE table_key = '...'` |

#### If it stops with an error

Every error says what to fix. The common ones are listed under **Checking a run** at the end of this section. Nothing is half-applied: each step runs in one transaction, so a failed run leaves the database exactly as it was.

> **Existing tables in `public`** built the old way (with duplicates and supplier columns) are not touched: step 2 stops with a clear message. Rename the old table (e.g. `ALTER TABLE public.customers RENAME TO customers_old;`) and let the first run build a clean one.

> **Practice first.** `tests/local/` has a worked example (`customer`) and an exercise (`payments`) with sample CSVs, so you can do all of the above against a local PostgreSQL before touching the real one.

### Everyday use

```
python main.py --pipeline config/tables/<table>.yaml                    # full run
python main.py --pipeline config/tables/<table>.yaml --file D:/in/x.csv # other file, same table
python main.py --pipeline config/tables/<table>.yaml --final-only       # rebuild analyst table only
python main.py --pipeline config/tables/<table>.yaml --setup-only       # apply sql/ files only
python main.py --pipeline config/tables/<table>.yaml --sql FILE.sql     # run a .sql file, print results
```

- **Renaming a column / changing a type / changing which rows are excluded** → edit the table's `_setup.sql`, re-apply it with `--sql`, then run with `--final-only`. (Or change the row in Postgres directly with any SQL client — the `_setup.sql` file is then out of date until you update it too.)
- **New table** → new YAML + new `_setup.sql`, apply, run.
- **Pause a table** → `UPDATE meta.etl_tables SET is_active = false WHERE table_key = '...'`.
- **Scheduling** → the weekly tables run the same one command from cron. Set the password in the crontab, since cron does not inherit your shell environment:
  ```
  0 3 * * 0 cd /opt/etl && PG_PASSWORD=secret .venv/bin/python main.py --pipeline config/tables/trans.yaml
  ```

#### `--sql` notes

- Runs the whole file in **one transaction**: if any statement fails, nothing in the file is applied.
- Prints a table for every `SELECT` in the file, so a check file like `tests/local/check_customer.sql` shows all its queries at once.
- Repeatable (`--sql a.sql --sql b.sql`); files run in the order given.
- Add `--autocommit` for statements Postgres refuses inside a transaction (`VACUUM`, `CREATE DATABASE`, `CREATE INDEX CONCURRENTLY`).

### YAML reference (`config/tables/*.yaml`)

| Key | Meaning |
|---|---|
| `pipeline.table_key` | The row in `meta.etl_tables`. Its presence is what makes `main.py` use this pipeline. |
| `source.type` | `trino` or `file` |
| `source.host / port / user / catalog / schema / table` | Trino source (`password` or `password_env` optional) |
| `source.path / separator / encoding` | File source. `--file` overrides `path`, so a file whose name changes still maps to the same table. |
| `source.chunk_size` | Rows per chunk (default 10000) |
| `postgres.host / port / database / user` | Target Postgres |
| `postgres.password_env` | Name of the environment variable holding the password (or `password`, not recommended) |

### `meta.etl_tables` reference

| Column | Meaning |
|---|---|
| `table_key` | Name you use in the YAML (usually the source table name) |
| `raw_schema`, `raw_table` | Raw table (`public` by default). The staging table gets the same name in `staging`. |
| `table_type` | `snapshot` or `insert_only` |
| `business_key` | Original column(s) identifying a record. Required for `snapshot`. |
| `fetch_time_col` | The supplier's fetch-time column: the only supplier column kept; never part of the comparison |
| `drop_cols` | Supplier extra columns, never stored |
| `final_schema`, `final_table` | Where the analyst table goes (Hebrew names allowed) |
| `exclude_where` | SQL condition on the **original** column names; matching rows are left out of the analyst table (they stay in `public`) |
| `pre_table_sql` | `text[]` of statements run on the staging table before it is loaded into `public`; `{table}` is the staging table |
| `keep_staging_days` | Keep each run's untouched staging copy for this many days (NULL = don't keep). Set it on any table with `pre_expr` / `pre_table_sql`. |
| `is_active` | `false` = skip this table |

### `meta.etl_column_map` reference (the mapper)

| Column | Meaning |
|---|---|
| `source_col` | Original name, as in `public` |
| `target_col` | Name in the analyst table. Max 63 bytes (≈31 Hebrew letters). |
| `target_type` | `text` (default), `integer`, `bigint`, `numeric`, `date`, `timestamp`, `boolean` |
| `source_format` | For `date` / `timestamp`: the text pattern, e.g. `DD/MM/YYYY` or `DD/MM/YYYY HH24:MI`. Empty = ISO. |
| `pre_expr` | Cleaning applied on the way **into** `public`; `{col}` is where the staging column goes. Not reversible without a staging copy. |
| `transform_expr` | Cleaning applied on the way to the analyst table, **before** the cast; `{col}` is where the raw column goes. See below. |
| `py_func` | Name of a Python function (`transform/column_functions.py`) run on this column after the final table is built. Needs `target_type = 'text'`. |
| `reason` | Why the column was renamed (documentation) |
| `ordinal` | Column order in the analyst table |
| `is_active` | `false` = leave the column out of the analyst table |

A column with no mapper row keeps its original name and stays text. Analysts query Hebrew names in double quotes: `SELECT "מספר_לקוח" FROM analyst."לקוחות" WHERE "גרסה_אחרונה";`

### Cleaning values (`transform_expr`)

Per-column cleaning lives in the mapper, next to the rename, and runs in step 3. `public` always keeps the supplier's original value, so a cleaning change costs one `--final-only` run and nothing is re-fetched.

Write `{col}` where the raw column goes:

```sql
UPDATE meta.etl_column_map
   SET transform_expr = 'meta.digits_only({col})'      -- '03-123456/7' -> '031234567'
 WHERE table_key = 'customer' AND source_col = 'person_id';
```

Ready-made functions (`sql/004_clean_functions.sql`, add your own there):

| Function | Does |
|---|---|
| `meta.digits_only({col})` | keep only 0-9 |
| `meta.letters_only({col})` | keep letters (Hebrew included), spaces, hyphens |
| `meta.squish({col})` | trim and collapse inner whitespace |
| `meta.blank_to_null({col})` | empty / whitespace-only → NULL |
| `meta.strip_leading_zeros({col})` | `'000123'` → `'123'` |
| `meta.zero_pad({col}, 9)` | pad on the left to a fixed width |
| `meta.keep_chars({col}, '0-9A-Z')` | keep only these characters |
| `meta.il_phone({col})` | digits only, `972…` → leading `0` |

Any SQL expression works too: `'upper(btrim({col}))'`, `'replace({col}, '''','''')'`, `'left(meta.digits_only({col}), 9)'`.

Notes:
- An id keeps its leading zeros only as `target_type = 'text'`; as a number `'031234567'` becomes `31234567`.
- If cleaning leaves nothing (`'לא-ידוע'` → digits only → NULL), that counts in `cast_failures`, so the run log tells you how many rows lost the value.
- A `transform_expr` without `{col}`, or one that is not valid SQL, stops the run with a message naming the column.

### Checking a run

```sql
SELECT table_key, started_at, staged_rows, raw_inserted, final_rows, cast_failures, status, error_detail
FROM meta.etl_runs ORDER BY started_at DESC LIMIT 20;
```

`cast_failures > 0` means some values could not be converted, or cleaning left them empty; the run log line names the column.

`tests/local/show_objects.sql` answers "does every table have its three tables, and how many rows are in each":

```
python main.py --pipeline config/tables/<any>.yaml --sql tests/local/show_objects.sql
```

| Message | Meaning / fix |
|---|---|
| `table_key 'x' has no row in meta.etl_tables` | Describe the table (setup step 4) |
| `... exists but was not created by this pipeline (no _row_hash)` | An old-style table is in the way — rename it |
| `fetch_time_col ... not found` / `business_key column(s) ... not found` | The column names in `meta.etl_tables` don't match the source |
| `SQL error: ... check exclude_where` | `exclude_where` is invalid or uses Hebrew/new names instead of original ones |
| `the columns of ... changed ... but a view depends on it` | A mapper change needs the analyst table recreated; drop the view, run, recreate the view |

---

## Features

- **Chunked streaming** — processes files of any size (1.5 GB+) with constant RAM usage (one 10 000-row chunk at a time)
- **Four load modes** — `replace`, `append`, `upsert`, `skip_existing`
- **Transform pipeline** — cleaning, validation, and parsing steps via a plug-in registry
- **Multi-database support** — writes to SQL Server (pyodbc), PostgreSQL (psycopg2), or AWS S3
- **Trino input** — read any Trino-accessible table (Hive, Iceberg, Delta Lake, etc.) as a streaming source in both the web UI and YAML pipelines
- **Audit log** — every run writes a row to `etl_audit_log` (auto-created, dialect-aware DDL)
- **Web UI** — Streamlit front-end for point-and-click ETL without touching YAML or a terminal
- **S3 destination** — writes Parquet chunks to AWS S3 via boto3

---

## Project Structure

```
ETL/
├── main.py                         # CLI entry point
├── requirements.txt                # Core dependencies
├── sql/                            # One-click pipeline SQL, applied automatically
│   ├── 001_meta_schema.sql         # meta.etl_tables, meta.etl_column_map, run + migration logs
│   ├── 002_load_raw.sql            # meta.load_raw(): staging → public
│   └── 003_build_final.sql         # meta.build_final(): public → analyst
├── config/
│   ├── pipeline.yaml               # Legacy YAML example
│   └── tables/                     # One-click YAML, one file per table
├── requirements-web.txt            # Web UI dependencies
├── .streamlit/
│   └── config.toml                 # Streamlit server settings
├── core/
│   ├── consts.py                   # Shared constants
│   ├── table_pipeline.py           # The runner: fetch → staging, load_raw, build_final, py_func
│   └── registry.py                 # Transform plug-in registry (@register decorator)
├── extract/
│   └── csv_reader.py               # stream_csv() — chunked CSV generator
├── transform/
│   ├── column_functions.py         # YOUR Python column functions (py_func)
│   ├── cleaners.py                 # trim_whitespace, normalize_nulls
│   ├── validators.py               # validate_required, validate_no_duplicates
│   └── parsers/
│       ├── date_parser.py
│       ├── email_parser.py
│       ├── phone_parser.py
│       └── numeric_parser.py
├── db/
│   ├── engine.py                   # SQLAlchemy engine factory
│   ├── migrations.py               # Applies sql/ files, records them in meta.schema_migrations
│   ├── sql_runner.py               # --sql: runs a .sql file and prints its results
│   └── loader.py                   # bulk_load() — SQL Server & PostgreSQL writer
├── utils/
│   ├── file_utils.py               # resolve_path(), assert_readable()
│   └── logging_utils.py
└── web/
    ├── app.py                      # Streamlit entry point (4-step UI)
    ├── config.py                   # Session state initialisation
    ├── components/
    │   ├── file_input.py           # Path / upload / Trino source selector
    │   ├── trino_input.py          # Trino connection form + cascading dropdowns
    │   ├── destination_form.py     # SQL Server / PostgreSQL / S3 credential forms
    │   ├── action_selector.py      # Action checkboxes from registry
    │   ├── batch_queue.py          # Queued jobs table
    │   ├── preview_table.py        # Data preview + column stats
    │   ├── progress_tracker.py     # Live progress bars
    │   └── sql_window.py           # SQL scratch pad (draft only)
    ├── services/
    │   ├── file_service.py         # Path validation, sampling, Excel chunking
    │   ├── trino_service.py        # Trino connection, listing, streaming helpers
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

**Requirements:** Python 3.11+.

- **SQL Server** target: an ODBC driver (e.g. [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)).
- **PostgreSQL** target: `psycopg2-binary` (included in `requirements-web.txt`). No extra system driver needed.

---

## Web UI (ETL Studio) and the pipeline

They share the same code for reading Trino and CSV, and nothing else. The UI is the old path: pick a source, pick actions, write to a destination.

**The UI will not write to a pipeline table.** If the target is any table listed in `meta.etl_tables` (its `public` or its analyst table), the job stops before writing with a message naming the table, the `table_key`, why it is protected, and the command to run instead. The same message is written to the log. The reason: a replace load would empty the table and destroy the version history that only `public` holds, and an append load would add rows the duplicate check never saw. Use the UI freely for any other table, and for previewing a Trino table before you configure it.

## Web UI Usage

```bash
streamlit run web/app.py
# Opens at http://localhost:8501
```

The UI walks through four steps:

| Step | What happens |
|------|-------------|
| **1 — Input & Destination** | Choose an input source (server path, file upload, or Trino table) and configure the SQL Server, PostgreSQL, or S3 destination. Add multiple sources to a batch queue. (Column renaming moved to the Postgres mapper — see the one-click pipeline.) |
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

All four modes are available for **SQL Server** and **PostgreSQL** destinations. S3 supports `append` and `replace` only (upsert and skip_existing require a database engine).

| Mode | Behaviour | Primary key required |
|------|-----------|:-------------------:|
| `replace` | Drop and recreate the table on the first chunk; append subsequent chunks | No |
| `append` | Always insert; create the table if it does not exist | No |
| `upsert` | Insert new rows; UPDATE existing rows matched on `primary_keys` | Yes |
| `skip_existing` | Insert only rows whose `primary_keys` are not already in the table | Yes |

### Weekly table updates

If you receive a table every week that contains **all previous rows plus new additions**, use **`skip_existing`** with your primary key column(s). The full table is loaded each time; rows already present in the destination are silently skipped and only the new rows are inserted.

Use **`upsert`** instead if existing rows may also change between deliveries — it inserts new rows and updates changed ones in a single pass.

---

## Database Engine

`db/engine.py` exposes `get_engine(connection_string=None)`:

- **CLI path** — calls your infra library (adapt the `MSSQLClient` import).
- **Web UI path** — pass a SQLAlchemy URL directly. Both dialects are supported:

**SQL Server**
```python
from db.engine import get_engine

engine = get_engine(
    "mssql+pyodbc:///?odbc_connect=driver=ODBC+Driver+17+for+SQL+Server;"
    "server=localhost;database=MyDB;uid=sa;pwd=secret"
)
```

**PostgreSQL**
```python
from db.engine import get_engine

engine = get_engine("postgresql+psycopg2://postgres:secret@localhost:5432/mydb")
```

The engine factory automatically passes `fast_executemany=True` only for MSSQL connections (a pyodbc-specific optimisation).

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

## PostgreSQL Destination

Configure in the web UI or build the destination dict directly:

```python
{
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "username": "postgres",
    "password": "secret",
    "table": "customers",
    "schema": "public",        # default: "public"
}
```

All four load modes (`replace`, `append`, `upsert`, `skip_existing`) work identically to SQL Server.
The audit log table is created with PostgreSQL-compatible DDL (`SERIAL`, `TEXT`, `TIMESTAMP`, `CREATE TABLE IF NOT EXISTS`).

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

## Trino Input

ETL Studio can read any table accessible via a Trino cluster — Hive, Iceberg, Delta Lake, TPCH, or any other Trino connector — and stream it into any supported destination.

### Installation

The Trino Python driver is included in `requirements-web.txt`. To install it standalone:

```bash
pip install trino
```

---

### Web UI

1. In **Step 1 — Input & Destination**, select the **Trino** radio button under "Input source".
2. Fill in your cluster details:

   | Field | Description |
   |-------|-------------|
   | **Trino host** | Hostname or IP of the Trino coordinator (e.g. `trino.example.com`) |
   | **Port** | HTTP port, default `8080` (use `443` for HTTPS clusters) |
   | **User** | Trino username |
   | **Password** | Optional. Leave blank for clusters without authentication. |

3. Click **Connect**. ETL Studio verifies the connection and populates the **Catalog** dropdown.
4. Select a **Catalog** — the Schema dropdown loads automatically.
5. Select a **Schema** (database) — the Table dropdown loads automatically.
6. Select a **Table** — the source is confirmed and the column renamer appears.
7. Continue through Steps 2–4 exactly as you would with a file source. Trino data is streamed in chunks of 10 000 rows so large tables never exhaust RAM.

> **Tip:** Column renames, all transform actions (cleaning, validation, parsing), and all load modes work identically with Trino as the source.

---

### YAML Pipeline (CLI)

Add `type: trino` to your `source` block. File-based pipelines that omit `type` continue to work unchanged.

```yaml
pipeline:
  name: hive_customers_import

source:
  type: trino               # "file" is the default; omit for file-based pipelines
  host: trino.example.com
  port: 8080                # default 8080
  user: admin
  password: secret          # optional; omit for unauthenticated clusters
  catalog: hive
  schema: sales
  table: customers
  chunk_size: 10000         # optional; default 10000

destination:
  table: customers
  schema: dbo
  load_mode: upsert
  primary_keys: [customer_id]

transform:
  steps:
    - trim_whitespace
    - normalize_nulls
    - name: validate_required
      params:
        columns: [customer_id, email]

audit:
  enabled: true
  schema: dbo
```

Run it with:

```bash
python main.py --pipeline config/pipeline.yaml
```

The `--file` override flag is ignored for Trino sources. The audit log records the source as `catalog.schema.table`.

---

### Authentication

| Cluster type | Configuration |
|---|---|
| No authentication | Omit `password` (or leave it blank in the UI) |
| Basic auth (username + password) | Set `password` in YAML / UI — uses `trino.auth.BasicAuthentication` |
| Other auth (JWT, OAuth2, Kerberos) | Not currently supported via the UI/YAML; extend `_connect()` in `web/services/trino_service.py` |

---

## Large File Handling

| Input method | RAM behaviour |
|---|---|
| Server path (CSV) | `stream_csv()` yields one chunk at a time — full file never in RAM |
| Server path (Excel) | `_stream_excel()` reads with `skiprows`/`nrows` — full file never in RAM |
| Upload | Streamed to `/tmp/etl_uploads/<session_id>/`, then treated as a server path |
| Trino | `cursor.fetchmany(chunk_size)` loop — result set never fully buffered |
| Preview | Reads only the first 1 000 rows |

Peak RAM usage = one chunk (default 10 000 rows) regardless of file size.
