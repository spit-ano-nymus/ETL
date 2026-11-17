-- =============================================================================
-- PRACTICE — the config file for the `payments` table. YOU fill it in.
--
-- This is the file you will write for every real table. It does two things:
--   part 1: one row describing the table      -> meta.etl_tables
--   part 2: one row per column                -> meta.etl_column_map
--
-- Apply it (safe to re-run after every edit):
--   python main.py --pipeline tests/local/payments_local.yaml --sql tests/local/payments_setup.sql
--
-- The supplier sends these columns (see tests/local/payments_jan.csv):
--   pay_ref, person_id, amount, pay_date, channel, notes,
--   extract_ts, src_batch_id, src_row_num
--
-- Look at the data first, then answer the five questions in part 1.
-- tests/local/setup_customer.sql is a filled-in example of this same file.
-- =============================================================================

DELETE FROM meta.etl_tables WHERE table_key = 'payments';   -- also clears its mapper rows


-- -----------------------------------------------------------------------------
-- PART 1 — the table
-- -----------------------------------------------------------------------------
-- Replace every <<< ... >>> below.
--
--  table_type        'snapshot' if a row can change later and you want to keep
--                    every version; 'insert_only' if a row never changes.
--  business_key      the column(s) that identify a record. Needed for
--                    'snapshot', leave NULL for 'insert_only'.
--  fetch_time_col    the supplier's "when I pulled this" column.
--  drop_cols         the supplier's own extra columns, which must not reach public.
--  final_schema/table where the analysts read it (Hebrew name is fine).
--  exclude_where     rows that should not reach the analysts. ORIGINAL column
--                    names. NULL to keep everything.
--  pre_table_sql     whole-table statements run on the staging table BEFORE it
--                    is loaded into public. {table} is the staging table.
--                    NULL if you don't need any.
--  keep_staging_days keep each run's untouched fetch for this many days. Set it
--                    whenever you change values before public.
-- -----------------------------------------------------------------------------

INSERT INTO meta.etl_tables
    (table_key, raw_table, table_type, business_key, fetch_time_col,
     drop_cols, final_schema, final_table, exclude_where,
     pre_table_sql, keep_staging_days, notes)
VALUES
    ('payments',
     'payments',
     'insert_only',                                    -- תשלום הוא אירוע, לא משתנה
     NULL,                                             -- אין מזהה רשומה ב-insert_only
     'extract_ts',                                     -- זמן השליפה של הספק
     ARRAY['src_batch_id', 'src_row_num'],             -- העמודות שלו, לא מגיעות ל-public
     'analyst', 'תשלומים',
     $$channel = 'refund'$$,                           -- זיכויים לא מגיעים לאנליסטים
     ARRAY[$$DELETE FROM {table} WHERE pay_ref !~ '^PAY/[0-9]{4}/[0-9]{4}$'$$],
     90,                                               -- שומר את ה-fetch של כל הרצה
     'practice table');


-- -----------------------------------------------------------------------------
-- PART 2 — the columns
-- -----------------------------------------------------------------------------
-- One row per column you want in the analyst table. A column with no row here
-- keeps its original name and stays text.
--
--  source_col      the name as the supplier sends it
--  target_col      the name the analysts see
--  target_type     text | integer | bigint | numeric | date | timestamp | boolean
--  source_format   only for date / timestamp, e.g. 'DD/MM/YYYY'
--  pre_expr        cleaning on the way INTO public.     {col} = the column
--  transform_expr  cleaning on the way to the analysts. {col} = the column
--  py_func         a Python function from transform/column_functions.py
--                  (needs target_type 'text')
--  reason          why you renamed it — this is the documentation
--  ordinal         column order in the analyst table
--
-- Ready-made cleaners are in sql/004_clean_functions.sql, among them
-- meta.digits_only, meta.squish, meta.blank_to_null, meta.zero_pad.
-- Python functions available: strip_nikud, normalize_space_py.
--
-- Hints, looking at the data:
--   pay_ref    arrives as PAY/2026/0001
--   person_id  arrives with hyphens and slashes, and one row has no digits at all
--   amount     arrives as text with thousands commas
--   pay_date   arrives as DD/MM/YYYY, and one row has an impossible date
--   notes      has Hebrew vowel points in one row and double spaces in another
-- -----------------------------------------------------------------------------

INSERT INTO meta.etl_column_map
    (table_key, source_col, target_col, target_type, source_format,
     pre_expr, transform_expr, py_func, reason, ordinal)
VALUES
    ('payments', 'pay_ref',    'אסמכתא',     'text',      NULL,
     NULL, NULL, NULL, 'שם ברור לאנליסט', 1),

    ('payments', 'person_id',  'תעודת_זהות', 'text',      NULL,
     'meta.digits_only({col})', NULL, NULL, 'הספק שולח עם מקפים', 2),

    ('payments', 'amount',     'סכום',       'numeric',   NULL,
     NULL, 'replace({col}, '','', '''')', NULL, 'מגיע כטקסט עם פסיקים', 3),

    ('payments', 'pay_date',   'תאריך',      'date',      'DD/MM/YYYY',
     NULL, NULL, NULL, 'מגיע כטקסט בפורמט יום/חודש/שנה', 4),

    ('payments', 'channel',    'ערוץ',       'text',      NULL,
     NULL, NULL, NULL, 'עברית לאנליסט', 5),

    ('payments', 'notes',      'הערות',      'text',      NULL,
     NULL, 'meta.squish({col})', 'strip_nikud', 'רווחים כפולים וניקוד', 6),

    ('payments', 'extract_ts', 'זמן_שליפה',  'timestamp', 'YYYY-MM-DD HH24:MI:SS',
     NULL, NULL, NULL, 'זמן השליפה של הספק', 7);
-- -----------------------------------------------------------------------------
-- See what you defined
-- -----------------------------------------------------------------------------
SELECT table_key, table_type, business_key, fetch_time_col, drop_cols,
       final_schema, final_table, exclude_where, pre_table_sql, keep_staging_days
FROM meta.etl_tables WHERE table_key = 'payments';

SELECT source_col, target_col, target_type, source_format,
       pre_expr, transform_expr, py_func, ordinal
FROM meta.etl_column_map WHERE table_key = 'payments' ORDER BY ordinal;
