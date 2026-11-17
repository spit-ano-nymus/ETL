-- =============================================================================
-- Describe the test table `customer`. This is the template for a real table:
-- one config file per table, applied from the terminal with
--
--   python main.py --pipeline tests/local/customer_local.yaml --sql tests/local/setup_customer.sql
--
-- Safe to run again: it replaces the customer config, so edit and re-apply
-- whenever a column name, a type or exclude_where changes.
--
-- The supplier file has these columns:
--   cust_no, full_name, city, open_date, status, extract_ts, src_batch_id, src_row_num
--   src_batch_id / src_row_num = supplier extra columns  -> dropped
--   extract_ts                 = supplier fetch time      -> kept, not compared
-- =============================================================================

DELETE FROM meta.etl_tables WHERE table_key = 'customer';   -- also removes its mapper rows

INSERT INTO meta.etl_tables
    (table_key, raw_table, table_type, business_key, fetch_time_col,
     drop_cols, final_schema, final_table, exclude_where, notes)
VALUES
    ('customer', 'customer', 'snapshot', ARRAY['cust_no'], 'extract_ts',
     ARRAY['src_batch_id', 'src_row_num'],
     'analyst', 'לקוחות',
     $$status = 'closed' OR cust_no LIKE 'TEST%'$$,       -- rows the analysts don't need
     'local test table');

INSERT INTO meta.etl_column_map
    (table_key, source_col, target_col, target_type, source_format, reason, ordinal)
VALUES
    ('customer', 'cust_no',    'מספר_לקוח',   'text',      NULL,                    'מזהה הלקוח',            1),
    ('customer', 'full_name',  'שם_מלא',      'text',      NULL,                    'עברית לאנליסט',          2),
    ('customer', 'city',       'עיר',         'text',      NULL,                    'עברית לאנליסט',          3),
    ('customer', 'open_date',  'תאריך_פתיחה', 'date',      'DD/MM/YYYY',            'מגיע כטקסט יום/חודש/שנה', 4),
    ('customer', 'status',     'סטטוס',       'text',      NULL,                    'עברית לאנליסט',          5),
    ('customer', 'extract_ts', 'זמן_שליפה',   'timestamp', 'YYYY-MM-DD HH24:MI:SS', 'זמן השליפה של הספק',     6);

-- -----------------------------------------------------------------------------
-- Cleaning a column (optional): transform_expr runs on the text before the
-- cast, and {col} is where the raw column goes. `public` keeps the original.
--
-- Uncomment, re-apply this file with --sql, then rebuild with --final-only
-- to see "  דנה   לוי " come out as "דנה לוי":
--
-- UPDATE meta.etl_column_map SET transform_expr = 'meta.squish({col})'
--  WHERE table_key = 'customer' AND source_col = 'full_name';
--
-- The same idea for an id the supplier sends with separators (03-123456/7):
--   transform_expr = 'meta.digits_only({col})'
-- Keep such an id as target_type 'text', not a number, or a leading zero is
-- lost; meta.zero_pad(meta.digits_only({col}), 9) restores a fixed width.
-- sql/004_clean_functions.sql lists the ready-made functions.
-- -----------------------------------------------------------------------------

SELECT * FROM meta.etl_tables WHERE table_key = 'customer';
SELECT * FROM meta.etl_column_map WHERE table_key = 'customer' ORDER BY ordinal;
