-- =============================================================================
-- Did the practice table come out right?
--
--   python main.py --pipeline tests/local/payments_local.yaml --sql tests/local/check_payments.sql
--
-- Every query prints its own table, with what to expect written above it.
-- =============================================================================

-- 1. The run log
SELECT started_at, source, staged_rows, raw_inserted, final_rows, cast_failures, status
FROM meta.etl_runs WHERE table_key = 'payments' ORDER BY started_at;
--  after jan: staged 6, raw_inserted 4, final_rows 3, cast_failures 1
--  after feb: staged 7, raw_inserted 2, final_rows 5, cast_failures 1
--
--  jan: 6 arrived. The TEST row is deleted before public, PAY/2026/0001 arrived
--       twice (identical once src_row_num is dropped) -> 4 reach public.
--       One of them is a refund, which the analysts don't get -> 3.
--  feb: 7 arrived. The TEST row goes, four rows were already in public
--       (a new extract_ts does not make a row new) -> only 0005 and 0006 are added.
--  cast_failures 1 = pay_date '31/02/2026' on PAY/2026/0003, which is not a real
--       date and becomes NULL. The row itself is kept.

-- 2. public — clean raw: no TEST row, no duplicates, no src_* columns,
--    original names, still text. person_id is already digits-only here,
--    because that cleaning was set as pre_expr.
SELECT * FROM public.payments ORDER BY pay_ref;
--  6 rows: PAY/2026/0001 .. 0006, including the refund

-- 3. analyst — Hebrew names, real types, refund gone, text cleaned
SELECT * FROM analyst."תשלומים" ORDER BY 1;
--  5 rows. 'סכום' is a number (1250.00, not '1,250.00'), 'תאריך' is a date,
--  PAY/2026/0003 has an empty date, 'הערות' has no vowel points and no double spaces

-- 4. The untouched fetch of every run, kept as a safety net (keep_staging_days)
SELECT snapshot_table, row_count, created_at FROM meta.staging_snapshots
WHERE table_key = 'payments' ORDER BY created_at;
--  one row per run, with the row count as it arrived (6, then 7)
--  look inside one with:  SELECT * FROM staging.payments_<stamp>;

-- 5. What the Python function was asked to do, and its cached answers
SELECT func_name, input_value, output_value FROM meta.py_func_cache ORDER BY func_name, input_value;
--  one row per distinct note; a second run of the same file calls nothing

-- 6. Do all three tables exist, and how many rows in each
SELECT t.table_key,
       (SELECT n FROM meta.count_rows('staging', t.raw_table) n)        AS staging_rows,
       (SELECT n FROM meta.count_rows(t.raw_schema, t.raw_table) n)     AS raw_rows,
       (SELECT n FROM meta.count_rows(t.final_schema, t.final_table) n) AS final_rows
FROM meta.etl_tables t WHERE t.table_key = 'payments';
--  staging 7 (the last fetch), raw 6, final 5
