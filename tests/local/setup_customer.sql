-- =============================================================================
-- Describe the test table `customer` (run once, after --setup-only).
-- Safe to run again: it replaces the customer config.
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

SELECT * FROM meta.etl_tables WHERE table_key = 'customer';
SELECT * FROM meta.etl_column_map WHERE table_key = 'customer' ORDER BY ordinal;
