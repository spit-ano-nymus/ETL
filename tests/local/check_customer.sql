-- =============================================================================
-- What to look at after each run, and what you should see.
-- Run the queries one by one in pgAdmin (select a query, then F5).
-- =============================================================================

-- 1. The run log: one row per run
SELECT started_at, source, staged_rows, raw_inserted, final_rows, cast_failures, status, error_detail
FROM meta.etl_runs WHERE table_key = 'customer' ORDER BY started_at;
--   after jan: staged 6, raw_inserted 5, final_rows 3, cast_failures 1
--   after mar: staged 6, raw_inserted 2, final_rows 5, cast_failures 1
--   after jun: staged 6, raw_inserted 1, final_rows 6, cast_failures 1
--   (jan: 1001 came twice -> once. mar: 1001 moved + 1005 new. jun: 1001 moved back.)
--   cast_failures 1 = the date 31/02/2021 of customer 1002 (it becomes NULL)

-- 2. staging: the last file exactly as it arrived (supplier columns still there)
SELECT * FROM staging.customer;

-- 3. public: clean raw — no duplicates, no src_batch_id / src_row_num, original names
SELECT * FROM public.customer ORDER BY cust_no, _raw_id;
--   after jun: 8 rows — 1001 x3 (Tel Aviv, Ramat Gan, Tel Aviv), 1002, 1003, 1004, 1005, TEST01

-- 4. analyst: Hebrew names, real types, no closed / TEST rows, all versions
SELECT * FROM analyst."לקוחות" ORDER BY "מספר_לקוח", "מספר_גרסה";
--   after jun: 6 rows — 1001 versions 1-3, 1002 (empty date), 1004 (empty date), 1005

-- 5. analyst: only the current state of each customer
SELECT "מספר_לקוח", "שם_מלא", "עיר", "תאריך_פתיחה"
FROM analyst."לקוחות" WHERE "גרסה_אחרונה" ORDER BY 1;
--   4 rows; 1001 is in Tel Aviv

-- 6. which sql/ files were installed
SELECT filename, applied_at FROM meta.schema_migrations ORDER BY filename;
