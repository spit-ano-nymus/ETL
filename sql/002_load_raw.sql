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
--     1. ignores the supplier's extra columns (drop_cols)
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
    hash_expr     text;            -- md5(row(...)::text) over hash_cols, staging alias s
    col_list      text;            -- "a", "b", ...
    sel_list      text;            -- s."a"::text, s."b"::text, ...
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
                        (SELECT string_agg(format('s.%I::text', c), ', ')
                           FROM unnest(hash_cols) c));
    col_list  := (SELECT string_agg(format('%I', c), ', ') FROM unnest(data_cols) c);
    sel_list  := (SELECT string_agg(format('s.%I::text', c), ', ') FROM unnest(data_cols) c);

    EXECUTE format('SELECT count(*) FROM %s', stg) INTO v_staged;

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
