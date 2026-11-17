-- =============================================================================
-- TEMPLATE — the config file for one table. Copy it to <table>_setup.sql
-- next to that table's YAML, fill it in, and apply it:
--
--   python main.py --pipeline config/tables/<table>.yaml --sql config/tables/<table>_setup.sql
--
-- It does two things:
--   part 1: one row describing the table   -> meta.etl_tables
--   part 2: one row per column             -> meta.etl_column_map
--
-- It starts with a DELETE, so it is safe to edit and re-apply as often as you
-- like: re-applying replaces this table's whole configuration.
--
-- After a change here, rebuild the analyst table without re-fetching:
--   python main.py --pipeline config/tables/<table>.yaml --final-only
-- unless the change is a pre_expr / pre_table_sql, which only affect rows
-- loaded from now on.
-- =============================================================================

DELETE FROM meta.etl_tables WHERE table_key = 'orders';   -- also clears its mapper rows


-- -----------------------------------------------------------------------------
-- Optional: a function used only by this table. A function has to exist in the
-- database before a column can call it, so it is defined here, above the rows
-- that reference it. Functions used by several tables belong in
-- sql/004_clean_functions.sql instead, which is applied automatically.
-- -----------------------------------------------------------------------------
-- CREATE OR REPLACE FUNCTION meta.orders_clean_ref(v text) RETURNS text
-- LANGUAGE sql IMMUTABLE AS
-- $$ SELECT replace(replace(btrim(v), 'ORD/', ''), '/', '-') $$;


-- -----------------------------------------------------------------------------
-- PART 1 — the table
-- -----------------------------------------------------------------------------
--  table_type        'snapshot'    a row can change; every version is kept
--                    'insert_only' a row never changes
--  business_key      column(s) identifying a record. Required for 'snapshot',
--                    NULL for 'insert_only'.
--  fetch_time_col    the supplier's "when I pulled this" column. Kept, and
--                    never part of the duplicate comparison.
--  drop_cols         the supplier's own extra columns; never stored.
--  final_schema/table  where the analysts read it (a Hebrew name is fine).
--  exclude_where     rows that should not reach the analysts. ORIGINAL column
--                    names. They stay in public. NULL = keep everything.
--  pre_table_sql     statements run on the staging table BEFORE it is loaded
--                    into public; {table} is the staging table. NULL = none.
--  keep_staging_days keep each run's untouched fetch this many days. Set it on
--                    any table that changes values before public, otherwise
--                    the supplier's original cannot be recovered.
-- -----------------------------------------------------------------------------

INSERT INTO meta.etl_tables
    (table_key, raw_table, table_type, business_key, fetch_time_col,
     drop_cols, final_schema, final_table, exclude_where,
     pre_table_sql, keep_staging_days, notes)
VALUES
    ('orders',                                   -- table_key, as used in every command
     'orders',                                   -- the table name in public
     'snapshot',
     ARRAY['order_id'],
     'extract_ts',
     ARRAY['src_batch_id', 'src_row_num'],
     'analyst', 'הזמנות',
     $$status = 'cancelled'$$,
     NULL,                                       -- e.g. ARRAY[$$DELETE FROM {table} WHERE ...$$]
     NULL,                                       -- e.g. 90
     'short note to yourself');


-- -----------------------------------------------------------------------------
-- PART 2 — the columns
-- -----------------------------------------------------------------------------
-- One row per column. A column with NO row here still reaches the analyst
-- table, under its original name and as text — to leave a column out, give it
-- a row with is_active = false.
--
--  target_type     text | integer | bigint | numeric | date | timestamp | boolean
--  source_format   date / timestamp only, e.g. 'DD/MM/YYYY'
--  pre_expr        cleaning on the way INTO public      ({col} = the column)
--  transform_expr  cleaning on the way to the analysts  ({col} = the column)
--  py_func         a function from transform/column_functions.py (needs 'text')
--  reason          why it was renamed — this is the documentation
--  ordinal         column order in the analyst table
--
-- Ready-made cleaners: sql/004_clean_functions.sql (meta.digits_only,
-- meta.squish, meta.blank_to_null, meta.zero_pad, meta.il_phone, …).
-- -----------------------------------------------------------------------------

INSERT INTO meta.etl_column_map
    (table_key, source_col, target_col, target_type, source_format,
     pre_expr, transform_expr, py_func, reason, ordinal)
VALUES
    ('orders', 'order_id',   'מספר_הזמנה', 'text',      NULL,
     NULL, NULL, NULL, 'מזהה ההזמנה', 1),

    ('orders', 'amount',     'סכום',       'numeric',   NULL,
     NULL, 'replace({col}, '','', '''')', NULL, 'מגיע כטקסט עם פסיקים', 2),

    ('orders', 'order_date', 'תאריך',      'date',      'DD/MM/YYYY',
     NULL, NULL, NULL, 'מגיע כטקסט', 3),

    ('orders', 'extract_ts', 'זמן_שליפה',  'timestamp', 'YYYY-MM-DD HH24:MI:SS',
     NULL, NULL, NULL, 'זמן השליפה של הספק', 4);

-- An id the supplier sends with separators, cleaned on the way into public.
-- Keep it 'text', not a number, or a leading zero is lost:
--  ('orders', 'person_id', 'תעודת_זהות', 'text', NULL,
--   'meta.digits_only({col})', NULL, NULL, 'הספק שולח עם מקפים', 5),

-- A column the analysts should not see (it stays in public):
--  ('orders', 'internal_cost', 'עלות', 'numeric', NULL,
--   NULL, NULL, NULL, 'פנימי בלבד', 6),   -- plus is_active = false, see below

-- A text column sent through a Python function (an API call, for example):
--  ('orders', 'notes', 'הערות', 'text', NULL,
--   NULL, 'meta.squish({col})', 'translate_he', 'תרגום', 7),


-- -----------------------------------------------------------------------------
-- Check what was stored
-- -----------------------------------------------------------------------------
SELECT table_key, table_type, business_key, fetch_time_col, drop_cols,
       final_schema, final_table, exclude_where, pre_table_sql, keep_staging_days
FROM meta.etl_tables WHERE table_key = 'orders';

SELECT source_col, target_col, target_type, source_format,
       pre_expr, transform_expr, py_func, is_active, ordinal
FROM meta.etl_column_map WHERE table_key = 'orders' ORDER BY ordinal;
