-- =============================================================================
-- 002_load_raw.sql
-- -----------------------------------------------------------------------------
-- Step 2 of the one-click pipeline: staging -> public raw.
--
--   Python streams the full fetch (Trino or file) into staging.<raw_table>,
--   every value as text, no PK, no dedup. Then it calls:
--
--       SELECT * FROM meta.load_raw('<table_key>');
--
--   load_raw reads meta.etl_tables and, for that table:
--     0. keeps this run's untouched staging copy, if keep_staging_days is set,
--        and drops copies past that age (meta.staging_snapshots)
--     1. ignores the supplier's extra columns (drop_cols), runs pre_table_sql
--        on the staging table, and applies each column's pre_expr
--     2. computes a row hash over every remaining column except fetch_time_col
--     3. inserts into raw only what is new:
--          snapshot     -> a record whose hash differs from its latest version
--          insert_only  -> a row whose hash does not exist yet
--
--   Raw gets two technical columns, original columns are untouched:
--     _raw_id    identity: insertion order + a real PK for raw
--     _row_hash  md5 of the hashed columns
--
-- Safe to re-run (CREATE OR REPLACE / IF NOT EXISTS).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;


CREATE OR REPLACE FUNCTION meta.load_raw(p_table_key text)
RETURNS TABLE (staged_rows bigint, inserted_rows bigint)
LANGUAGE plpgsql
AS $fn$
DECLARE
    cfg           meta.etl_tables%ROWTYPE;
    stg           text;            -- staging.<raw_table>, quoted
    raw           text;            -- <raw_schema>.<raw_table>, quoted
    data_cols     text[];          -- staging columns kept in raw (order of staging)
    hash_cols     text[];          -- data_cols minus fetch_time_col, sorted by name
    missing       text[];
    col           text;
    pre_cols      text[];          -- columns that have a pre_expr
    pre_exprs     text[];          -- their expressions, same order
    expr_arr      text[];          -- per data_col: the expression read from staging
    idx           integer;
    e             text;
    stmt          text;
    snap_name     text;
    snap          text;            -- staging.<raw_table>_<timestamp>, this run's copy
    dropped_snaps integer := 0;
    hash_expr     text;            -- md5(row(...)::text) over hash_cols, after pre_expr
    col_list      text;            -- "a", "b", ...
    sel_list      text;            -- the per-column expressions, in data_cols order
    key_cols      text;            -- "k1", "k2"
    key_join      text;            -- null-safe equality of the business key
    v_staged      bigint;
    v_inserted    bigint;
BEGIN
    -- ── config ───────────────────────────────────────────────────────────────
    SELECT * INTO cfg FROM meta.etl_tables WHERE table_key = p_table_key;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'load_raw: no row in meta.etl_tables for table_key %', p_table_key;
    END IF;
    IF NOT cfg.is_active THEN
        RAISE EXCEPTION 'load_raw: table_key % is marked inactive', p_table_key;
    END IF;

    stg := format('%I.%I', 'staging', cfg.raw_table);
    raw := format('%I.%I', cfg.raw_schema, cfg.raw_table);

    IF to_regclass(stg) IS NULL THEN
        RAISE EXCEPTION 'load_raw: staging table % does not exist — did the fetch run?', stg;
    END IF;

    -- ── columns: staging minus supplier extras ───────────────────────────────
    SELECT array_agg(c.column_name::text ORDER BY c.ordinal_position)
      INTO data_cols
      FROM information_schema.columns c
     WHERE c.table_schema = 'staging'
       AND c.table_name   = cfg.raw_table
       AND NOT (c.column_name::text = ANY (cfg.drop_cols));

    IF NOT (cfg.fetch_time_col = ANY (data_cols)) THEN
        RAISE EXCEPTION 'load_raw: fetch_time_col % not found in %', cfg.fetch_time_col, stg;
    END IF;

    IF cfg.table_type = 'snapshot' THEN
        SELECT array_agg(k) INTO missing
          FROM unnest(cfg.business_key) k
         WHERE NOT (k = ANY (data_cols));
        IF missing IS NOT NULL THEN
            RAISE EXCEPTION 'load_raw: business_key column(s) % not found in %', missing, stg;
        END IF;
    END IF;

    SELECT array_agg(c ORDER BY c) INTO hash_cols
      FROM unnest(data_cols) c
     WHERE c <> cfg.fetch_time_col;

    IF hash_cols IS NULL THEN
        RAISE EXCEPTION 'load_raw: % has no columns to hash besides %', stg, cfg.fetch_time_col;
    END IF;

    EXECUTE format('SELECT count(*) FROM %s', stg) INTO v_staged;   -- as the supplier sent it

    -- ── this run's untouched staging copy (safety net for before-public logic) ─
    SELECT array_agg(m.source_col), array_agg(btrim(m.pre_expr))
      INTO pre_cols, pre_exprs
      FROM meta.etl_column_map m
     WHERE m.table_key = p_table_key
       AND nullif(btrim(coalesce(m.pre_expr, '')), '') IS NOT NULL;

    IF cfg.keep_staging_days IS NOT NULL AND cfg.keep_staging_days > 0 THEN
        -- left(…, 40): an identifier is capped at 63 bytes, so keep room for the stamp
        snap := left(cfg.raw_table, 40) || '_' || to_char(clock_timestamp(), 'YYYYMMDD_HH24MISS');
        idx := 1;
        snap_name := snap;
        WHILE to_regclass(format('%I.%I', 'staging', snap_name)) IS NOT NULL LOOP
            idx := idx + 1;                      -- same second as a previous run
            snap_name := snap || '_' || idx;
        END LOOP;
        snap := snap_name;
        EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %s', 'staging', snap, stg);
        EXECUTE format('INSERT INTO meta.staging_snapshots (snapshot_table, table_key, row_count) '
                       'SELECT %L, %L, count(*) FROM %I.%I',
                       snap, p_table_key, 'staging', snap);
        RAISE NOTICE 'load_raw %: kept this run''s fetch as staging.%', p_table_key, snap;

        -- drop copies past their keep-days
        FOR snap_name IN
            SELECT snapshot_table FROM meta.staging_snapshots
             WHERE table_key = p_table_key
               AND created_at < now() - make_interval(days => cfg.keep_staging_days)
        LOOP
            EXECUTE format('DROP TABLE IF EXISTS %I.%I', 'staging', snap_name);
            DELETE FROM meta.staging_snapshots WHERE snapshot_table = snap_name;
            dropped_snaps := dropped_snaps + 1;
        END LOOP;
        IF dropped_snaps > 0 THEN
            RAISE NOTICE 'load_raw %: dropped % staging copy/copies older than % day(s)',
                         p_table_key, dropped_snaps, cfg.keep_staging_days;
        END IF;
    ELSIF pre_cols IS NOT NULL OR cfg.pre_table_sql IS NOT NULL THEN
        RAISE WARNING 'load_raw %: this table changes values before public but keep_staging_days '
                      'is not set, so the supplier''s original cannot be recovered', p_table_key;
    END IF;

    -- ── whole-table statements on staging, before anything is loaded ──────────
    IF cfg.pre_table_sql IS NOT NULL THEN
        FOREACH stmt IN ARRAY cfg.pre_table_sql LOOP
            IF nullif(btrim(stmt), '') IS NULL THEN CONTINUE; END IF;
            IF position('{table}' in stmt) = 0 THEN
                RAISE EXCEPTION 'load_raw %: pre_table_sql must contain {table} (the staging table), got: %',
                                p_table_key, stmt;
            END IF;
            BEGIN
                EXECUTE replace(stmt, '{table}', stg);
            EXCEPTION WHEN others THEN
                RAISE EXCEPTION 'load_raw %: pre_table_sql failed (%) in statement: %',
                                p_table_key, SQLERRM, stmt;
            END;
            RAISE NOTICE 'load_raw %: pre_table_sql ran: %', p_table_key, left(btrim(stmt), 80);
        END LOOP;
    END IF;

    -- ── per-column expression read from staging (pre_expr applied here) ───────
    expr_arr := '{}';
    FOREACH col IN ARRAY data_cols LOOP
        idx := CASE WHEN pre_cols IS NULL THEN NULL ELSE array_position(pre_cols, col) END;
        IF idx IS NULL THEN
            e := format('s.%I::text', col);
        ELSE
            IF position('{col}' in pre_exprs[idx]) = 0 THEN
                RAISE EXCEPTION 'load_raw %: pre_expr for column % must contain {col} '
                                '(it marks where the column goes), got: %',
                                p_table_key, col, pre_exprs[idx];
            END IF;
            e := '(' || replace(pre_exprs[idx], '{col}', format('s.%I', col)) || ')::text';
        END IF;
        expr_arr := expr_arr || e;
    END LOOP;

    -- a pre_expr pointing at a column the fetch does not have
    IF pre_cols IS NOT NULL THEN
        SELECT array_agg(c) INTO missing FROM unnest(pre_cols) c WHERE NOT (c = ANY (data_cols));
        IF missing IS NOT NULL THEN
            RAISE WARNING 'load_raw %: pre_expr set for column(s) % which are not in this fetch — ignored',
                          p_table_key, missing;
        END IF;
    END IF;

    -- ── raw table: create on first run, add new source columns later ─────────
    IF to_regclass(raw) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %s (%s, _row_hash text NOT NULL, '
            '_raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY)',
            raw,
            (SELECT string_agg(format('%I text', c), ', ') FROM unnest(data_cols) c)
        );
        IF cfg.table_type = 'insert_only' THEN
            EXECUTE format('CREATE UNIQUE INDEX %I ON %s (_row_hash)',
                           cfg.raw_table || '_row_hash_uq', raw);
        ELSE
            EXECUTE format('CREATE INDEX %I ON %s (%s, _raw_id DESC)',
                           cfg.raw_table || '_key_latest_ix', raw,
                           (SELECT string_agg(format('%I', k), ', ')
                              FROM unnest(cfg.business_key) k));
        END IF;
    ELSE
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                        WHERE table_schema = cfg.raw_schema
                          AND table_name   = cfg.raw_table
                          AND column_name  = '_row_hash') THEN
            RAISE EXCEPTION
                'load_raw: % exists but was not created by this pipeline (no _row_hash). '
                'It needs a one-time migration before it can be loaded this way.', raw;
        END IF;

        -- a column the source started sending: add it to raw as text
        FOREACH col IN ARRAY data_cols LOOP
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                            WHERE table_schema = cfg.raw_schema
                              AND table_name   = cfg.raw_table
                              AND column_name  = col) THEN
                EXECUTE format('ALTER TABLE %s ADD COLUMN %I text', raw, col);
                RAISE NOTICE 'load_raw: added new column % to %', col, raw;
            END IF;
        END LOOP;
    END IF;

    -- ── building blocks ──────────────────────────────────────────────────────
    hash_expr := format('md5(row(%s)::text)',
                        (SELECT string_agg(expr_arr[array_position(data_cols, c)], ', ' ORDER BY c)
                           FROM unnest(hash_cols) c));
    col_list  := (SELECT string_agg(format('%I', c), ', ') FROM unnest(data_cols) c);
    sel_list  := array_to_string(expr_arr, ', ');

    -- staging was just filled and raw may have grown: refresh planner
    -- statistics, otherwise Postgres can pick a plan that takes minutes
    EXECUTE format('ANALYZE %s', stg);
    EXECUTE format('ANALYZE %s', raw);

    -- ── load ─────────────────────────────────────────────────────────────────
    IF cfg.table_type = 'insert_only' THEN
        -- one row per hash (earliest fetch time wins), skip hashes already in raw
        EXECUTE format($q$
            INSERT INTO %1$s (%2$s, _row_hash)
            SELECT DISTINCT ON (h) %3$s, h
              FROM (SELECT s.*, %4$s AS h FROM %5$s s) s
             ORDER BY h, s.%6$I
            ON CONFLICT (_row_hash) DO NOTHING
        $q$, raw, col_list, sel_list, hash_expr, stg, cfg.fetch_time_col);
        GET DIAGNOSTICS v_inserted = ROW_COUNT;

    ELSE  -- snapshot
        key_cols := (SELECT string_agg(format('%I', k), ', ') FROM unnest(cfg.business_key) k);
        -- compare keys through a hash: null-safe and still a plain equality,
        -- so Postgres can use a hash join on large tables
        key_join := format('md5(row(%s)::text) = md5(row(%s)::text)',
                           (SELECT string_agg(format('s.%I::text', k), ', ') FROM unnest(cfg.business_key) k),
                           (SELECT string_agg(format('l.%I::text', k), ', ') FROM unnest(cfg.business_key) k));

        -- one row per record from this fetch (latest fetch time wins),
        -- inserted only if it differs from that record's latest version in raw
        EXECUTE format($q$
            INSERT INTO %1$s (%2$s, _row_hash)
            SELECT %3$s, s.h
              FROM (SELECT DISTINCT ON (%4$s) s.*, %5$s AS h
                      FROM %6$s s
                     ORDER BY %4$s, s.%7$I DESC) s
              LEFT JOIN (SELECT DISTINCT ON (%4$s) %4$s, _row_hash
                           FROM %1$s
                          ORDER BY %4$s, _raw_id DESC) l
                ON %8$s
             WHERE l._row_hash IS DISTINCT FROM s.h
        $q$, raw, col_list, sel_list, key_cols, hash_expr, stg,
             cfg.fetch_time_col, key_join);
        GET DIAGNOSTICS v_inserted = ROW_COUNT;
    END IF;

    RAISE NOTICE 'load_raw %: % staged, % inserted into %',
                 p_table_key, v_staged, v_inserted, raw;

    staged_rows   := v_staged;
    inserted_rows := v_inserted;
    RETURN NEXT;
END
$fn$;

COMMENT ON FUNCTION meta.load_raw(text) IS
    'Step 2: move one fetch from staging.<raw_table> into raw, keeping only new rows / changed versions.';
