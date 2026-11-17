-- =============================================================================
-- "What exists in the database, and does every table have its three tables?"
--
--   python main.py --pipeline <any table yaml> --sql queries/show_objects.sql
--
-- Works on any database the pipeline manages — it reads meta.etl_tables.
-- =============================================================================

-- 1. Every configured table and its three tables (missing = not loaded yet)
SELECT t.table_key,
       t.table_type,
       CASE WHEN to_regclass('staging.' || quote_ident(t.raw_table)) IS NULL
            THEN '-' ELSE 'yes' END                                   AS staging,
       CASE WHEN to_regclass(quote_ident(t.raw_schema) || '.' || quote_ident(t.raw_table)) IS NULL
            THEN '-' ELSE 'yes' END                                   AS raw_public,
       CASE WHEN to_regclass(quote_ident(t.final_schema) || '.' || quote_ident(t.final_table)) IS NULL
            THEN '-' ELSE 'yes' END                                   AS final_analyst,
       t.raw_schema || '.' || t.raw_table                             AS raw_name,
       t.final_schema || '.' || t.final_table                         AS final_name,
       t.is_active
FROM meta.etl_tables t
ORDER BY t.table_key;

-- 2. Row counts, live
SELECT t.table_key,
       (SELECT n FROM meta.count_rows('staging', t.raw_table) n)            AS staging_rows,
       (SELECT n FROM meta.count_rows(t.raw_schema, t.raw_table) n)         AS raw_rows,
       (SELECT n FROM meta.count_rows(t.final_schema, t.final_table) n)     AS final_rows
FROM meta.etl_tables t
ORDER BY t.table_key;

-- 3. Every table in the three schemas, including ones the pipeline does not manage
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('staging', 'public', 'analyst', 'meta')
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

-- 4. The last run of each table
SELECT DISTINCT ON (table_key)
       table_key, started_at, staged_rows, raw_inserted, final_rows,
       cast_failures, status, left(coalesce(error_detail, ''), 60) AS error
FROM meta.etl_runs
ORDER BY table_key, started_at DESC;
