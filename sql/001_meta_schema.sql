-- =============================================================================
-- 001_meta_schema.sql
-- -----------------------------------------------------------------------------
-- Configuration for the one-click pipeline (Trino -> public raw -> final).
--
--   meta.etl_tables      one row per source table: how it is loaded and where
--                        it ends up
--   meta.etl_column_map  the column mapper: original name -> analyst-facing
--                        name, target type, and why it was renamed
--   meta.etl_runs        run log, one row per run
--   meta.schema_migrations  which sql/ files the runner applied
--
-- Safe to re-run (IF NOT EXISTS everywhere). Does not touch existing data.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS meta;


-- -----------------------------------------------------------------------------
-- meta.etl_tables
-- -----------------------------------------------------------------------------
-- table_type:
--   snapshot     the supplier sends the full table on every fetch. A record
--                (identified by business_key) gets a new row in raw only when
--                its values differ from that record's latest stored version.
--   insert_only  rows never change. A row is added only if an identical row
--                (all columns except fetch_time_col) does not exist yet.
--
-- All column names here are ORIGINAL (source) names, as they appear in raw.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.etl_tables (
    table_key       text        PRIMARY KEY,                -- source table name in Trino
    raw_schema      text        NOT NULL DEFAULT 'public',
    raw_table       text        NOT NULL,
    table_type      text        NOT NULL
                    CHECK (table_type IN ('snapshot', 'insert_only')),
    business_key    text[],                                 -- required for snapshot tables
    fetch_time_col  text        NOT NULL,                   -- supplier fetch time: kept, never hashed
    drop_cols       text[]      NOT NULL DEFAULT '{}',      -- supplier extra columns, dropped before raw
    final_schema    text        NOT NULL,
    final_table     text        NOT NULL,
    exclude_where   text,                                   -- rows matching this are left out of final
    pre_table_sql   text[],                                 -- statements run on the staging table before loading; {table} = it
    keep_staging_days integer,                              -- keep each run's staging copy this many days (NULL = don't keep)
    is_active       boolean     NOT NULL DEFAULT true,
    notes           text,
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT etl_tables_raw_uq   UNIQUE (raw_schema, raw_table),
    CONSTRAINT etl_tables_final_uq UNIQUE (final_schema, final_table),
    CONSTRAINT etl_tables_snapshot_needs_key
        CHECK (table_type <> 'snapshot' OR coalesce(cardinality(business_key), 0) > 0),
    CONSTRAINT etl_tables_fetch_time_not_dropped
        CHECK (NOT (fetch_time_col = ANY (drop_cols)))
);

-- Columns added after the first version. CREATE TABLE IF NOT EXISTS above does
-- nothing when the table already exists, so an existing database gets them here.
-- These must come BEFORE the COMMENTs that describe them.
ALTER TABLE meta.etl_tables ADD COLUMN IF NOT EXISTS pre_table_sql     text[];
ALTER TABLE meta.etl_tables ADD COLUMN IF NOT EXISTS keep_staging_days integer;

COMMENT ON TABLE  meta.etl_tables                IS 'One row per source table loaded by the pipeline.';
COMMENT ON COLUMN meta.etl_tables.table_type     IS 'snapshot = keep a new version when values change; insert_only = add only rows not seen before.';
COMMENT ON COLUMN meta.etl_tables.business_key   IS 'Original column names that identify a record (snapshot tables).';
COMMENT ON COLUMN meta.etl_tables.fetch_time_col IS 'Supplier fetch-time column: the only supplier column kept; excluded from the row hash.';
COMMENT ON COLUMN meta.etl_tables.drop_cols      IS 'Supplier-added columns removed before loading to raw.';
COMMENT ON COLUMN meta.etl_tables.exclude_where  IS 'SQL condition on raw (original) column names; matching rows are left out of the final table. NULL = keep all.';
COMMENT ON COLUMN meta.etl_tables.pre_table_sql IS 'Whole-table statements run on the staging table before it is loaded into raw; {table} is the staging table.';
COMMENT ON COLUMN meta.etl_tables.keep_staging_days IS 'Keep a copy of each run''s staging table for this many days (see meta.staging_snapshots). Set it on any table with before-public logic.';


-- -----------------------------------------------------------------------------
-- meta.etl_column_map  (the mapper)
-- -----------------------------------------------------------------------------
-- Raw keeps every value as text. target_type says how the final layer casts
-- it; source_format is the text pattern for dates/timestamps (to_date format,
-- e.g. 'DD/MM/YYYY'). Columns with no mapper row keep their original name and
-- stay text.
--
-- transform_expr cleans the text BEFORE the cast, per column. Write {col}
-- where the raw column goes, e.g.
--     meta.digits_only({col})            keep only 0-9  (see 004_clean_functions.sql)
--     meta.squish({col})                 trim + collapse inner spaces
--     upper(btrim({col}))                any SQL expression is allowed
-- Raw keeps the original value either way; only the analyst table is cleaned.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.etl_column_map (
    table_key      text     NOT NULL
                   REFERENCES meta.etl_tables (table_key)
                   ON UPDATE CASCADE ON DELETE CASCADE,
    source_col     text     NOT NULL,                       -- original name (raw)
    target_col     text     NOT NULL,                       -- name in the final table
    target_type    text     NOT NULL DEFAULT 'text'
                   CHECK (target_type IN ('text', 'integer', 'bigint', 'numeric',
                                          'date', 'timestamp', 'boolean')),
    source_format  text,                                    -- for date / timestamp
    pre_expr       text,                                    -- cleaning on the way INTO public; {col} = the staging column
    transform_expr text,                                    -- cleaning before the cast; {col} = the raw column
    py_func        text,                                    -- Python function applied after the final table is built
    reason         text,                                    -- why it was renamed
    ordinal        integer,                                 -- column order in final
    is_active      boolean  NOT NULL DEFAULT true,          -- false = leave out of final

    CONSTRAINT etl_column_map_pk        PRIMARY KEY (table_key, source_col),
    CONSTRAINT etl_column_map_target_uq UNIQUE (table_key, target_col),
    -- Postgres cuts identifiers at 63 bytes; a Hebrew letter is 2 bytes,
    -- so a Hebrew name can be at most ~31 letters
    CONSTRAINT etl_column_map_target_len CHECK (octet_length(target_col) <= 63)
);

-- Columns added after the first version (see the note above).
ALTER TABLE meta.etl_column_map ADD COLUMN IF NOT EXISTS transform_expr text;
ALTER TABLE meta.etl_column_map ADD COLUMN IF NOT EXISTS pre_expr       text;
ALTER TABLE meta.etl_column_map ADD COLUMN IF NOT EXISTS py_func        text;

COMMENT ON TABLE  meta.etl_column_map                IS 'Column mapper: original name -> final name, type, cleaning and reason.';
COMMENT ON COLUMN meta.etl_column_map.transform_expr IS 'SQL expression cleaning the value before the cast (public -> analyst); {col} stands for the raw column. e.g. meta.digits_only({col})';
COMMENT ON COLUMN meta.etl_column_map.pre_expr       IS 'SQL expression cleaning the value on the way INTO public (staging -> public); {col} stands for the staging column. Not reversible without a staging copy.';
COMMENT ON COLUMN meta.etl_column_map.py_func        IS 'Name of a registered Python function run on this column after the final table is built (for API calls such as translation). See transform/column_functions.py.';


-- -----------------------------------------------------------------------------
-- meta.etl_runs  — one row per run (written by the Python runner)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.etl_runs (
    run_id         uuid        PRIMARY KEY,
    table_key      text        NOT NULL,
    source         text,
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz,
    staged_rows    bigint,                 -- rows the supplier sent
    raw_inserted   bigint,                 -- new rows / versions added to raw
    final_rows     bigint,                 -- rows in the final table after the run
    cast_failures  bigint,                 -- values that could not be converted (became NULL)
    status         text        NOT NULL,   -- running | success | failure
    error_detail   text
);

CREATE INDEX IF NOT EXISTS etl_runs_table_ix ON meta.etl_runs (table_key, started_at DESC);

COMMENT ON TABLE meta.etl_runs IS 'Run log: one row per pipeline run.';


-- -----------------------------------------------------------------------------
-- meta.schema_migrations  — which sql/ files were run, when, and their content
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.schema_migrations (
    filename    text        PRIMARY KEY,
    checksum    text        NOT NULL,      -- md5 of the file: a changed file runs again
    applied_at  timestamptz NOT NULL DEFAULT now(),
    content     text        NOT NULL
);

COMMENT ON TABLE meta.schema_migrations IS 'sql/ files applied by the runner, with their content at the time.';


-- -----------------------------------------------------------------------------
-- meta.staging_snapshots  — the per-run copies of staging kept as a safety net
-- -----------------------------------------------------------------------------
-- A table with before-public logic (pre_expr / pre_table_sql) changes values
-- on their way into public, which cannot be undone from public itself. With
-- keep_staging_days set, every run leaves its untouched fetch here, and copies
-- older than that many days are dropped automatically on the next run.
CREATE TABLE IF NOT EXISTS meta.staging_snapshots (
    snapshot_table text        PRIMARY KEY,     -- table name inside the staging schema
    table_key      text        NOT NULL,
    run_id         uuid,
    row_count      bigint,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS staging_snapshots_key_ix
    ON meta.staging_snapshots (table_key, created_at DESC);

COMMENT ON TABLE meta.staging_snapshots IS 'Per-run copies of the staging table, kept for keep_staging_days.';


-- -----------------------------------------------------------------------------
-- meta.py_func_cache  — results of Python column functions (e.g. translations)
-- -----------------------------------------------------------------------------
-- The final table is rebuilt on every run, so without a cache every run would
-- call the API again for values it has already seen. Keyed by the md5 of the
-- input, because an input value can be longer than an index key allows.
CREATE TABLE IF NOT EXISTS meta.py_func_cache (
    func_name    text        NOT NULL,
    input_md5    text        NOT NULL,
    input_value  text,
    output_value text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT py_func_cache_pk PRIMARY KEY (func_name, input_md5)
);

COMMENT ON TABLE meta.py_func_cache IS 'Cached output of Python column functions, so an API is called once per distinct value.';


-- -----------------------------------------------------------------------------
-- Example (commented out) — one snapshot table
-- -----------------------------------------------------------------------------
-- INSERT INTO meta.etl_tables
--     (table_key, raw_table, table_type, business_key, fetch_time_col,
--      drop_cols, final_schema, final_table, exclude_where)
-- VALUES
--     ('customers', 'customers', 'snapshot', ARRAY['cust_no'], 'extract_ts',
--      ARRAY['src_batch_id', 'src_row_num'], 'analyst', 'לקוחות',
--      $$status = 'closed' OR cust_no LIKE 'TEST%'$$);
--
-- INSERT INTO meta.etl_column_map
--     (table_key, source_col, target_col, target_type, source_format, reason, ordinal)
-- VALUES
--     ('customers', 'cust_no',    'מספר_לקוח',   'bigint',    NULL,                    'שם ברור לאנליסט', 1),
--     ('customers', 'open_dt',    'תאריך_פתיחה', 'date',      'DD/MM/YYYY',            'מגיע כטקסט',      2),
--     ('customers', 'extract_ts', 'זמן_שליפה',   'timestamp', 'YYYY-MM-DD HH24:MI:SS', 'זמן השליפה של הספק', 99);
