-- =============================================================================
-- 003_build_final.sql
-- -----------------------------------------------------------------------------
-- Step 3 of the one-click pipeline: public raw -> analyst (final) table.
--
--       SELECT * FROM meta.build_final('<table_key>');
--
--   build_final rebuilds the final table from raw on every run, so a change
--   in the mapper or in exclude_where applies to all history right away:
--     1. renames columns and converts their type by meta.etl_column_map
--        (a value that cannot be converted becomes NULL and is counted)
--     2. leaves out the rows that match exclude_where (meta.etl_tables)
--     3. snapshot tables: keeps every version and adds two columns,
--        מספר_גרסה (1 = first version of the record) and
--        גרסה_אחרונה (true on the record's latest version)
--
--   If the final table's columns did not change, it is emptied and refilled
--   (views on it and permissions stay). If they changed, it is recreated.
--
-- Safe to re-run (CREATE OR REPLACE).
-- =============================================================================


-- ── safe conversions: return NULL instead of failing on dirty text ──────────

CREATE OR REPLACE FUNCTION meta.try_integer(v text) RETURNS integer
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    RETURN btrim(v)::integer;
EXCEPTION WHEN others THEN RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION meta.try_bigint(v text) RETURNS bigint
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    RETURN btrim(v)::bigint;
EXCEPTION WHEN others THEN RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION meta.try_numeric(v text) RETURNS numeric
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    RETURN btrim(v)::numeric;
EXCEPTION WHEN others THEN RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION meta.try_boolean(v text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    RETURN btrim(v)::boolean;          -- true/false, t/f, yes/no, 1/0
EXCEPTION WHEN others THEN RETURN NULL;
END $$;

-- fmt: a to_date pattern such as 'DD/MM/YYYY'; NULL = ISO (YYYY-MM-DD)
CREATE OR REPLACE FUNCTION meta.try_date(v text, fmt text) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    IF fmt IS NULL THEN RETURN btrim(v)::date; END IF;
    RETURN to_date(btrim(v), fmt);
EXCEPTION WHEN others THEN RETURN NULL;
END $$;

-- fmt: a to_timestamp pattern such as 'DD/MM/YYYY HH24:MI'; NULL = ISO
CREATE OR REPLACE FUNCTION meta.try_timestamp(v text, fmt text) RETURNS timestamp
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL OR btrim(v) = '' THEN RETURN NULL; END IF;
    IF fmt IS NULL THEN RETURN btrim(v)::timestamp; END IF;
    RETURN to_timestamp(btrim(v), fmt)::timestamp;
EXCEPTION WHEN others THEN RETURN NULL;
END $$;


-- ── build_final ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION meta.build_final(p_table_key text)
RETURNS TABLE (final_rows bigint, cast_failures bigint)
LANGUAGE plpgsql
AS $fn$
DECLARE
    cfg            meta.etl_tables%ROWTYPE;
    raw            text;
    fin            text;
    rec            record;
    expr           text;
    pg_type        text;          -- type as Postgres reports it, for comparison
    sel_items      text[] := '{}';
    col_defs       text[] := '{}';
    sig_items      text[] := '{}';
    fail_conds     text[] := '{}';
    fail_cols      text[] := '{}';
    dupes          text[];
    stale          text[];
    key_cols       text;
    src_sql        text;
    want_sig       text;
    have_sig       text;
    fail_counts    bigint[];
    v_rows         bigint;
    v_fail         bigint := 0;
    i              int;
BEGIN
    -- ── config ───────────────────────────────────────────────────────────────
    SELECT * INTO cfg FROM meta.etl_tables WHERE table_key = p_table_key;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'build_final: no row in meta.etl_tables for table_key %', p_table_key;
    END IF;

    raw := format('%I.%I', cfg.raw_schema, cfg.raw_table);
    fin := format('%I.%I', cfg.final_schema, cfg.final_table);

    IF to_regclass(raw) IS NULL THEN
        RAISE EXCEPTION 'build_final: raw table % does not exist — run meta.load_raw first', raw;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = cfg.final_schema) THEN
        EXECUTE format('CREATE SCHEMA %I', cfg.final_schema);
    END IF;

    -- mapper rows that point at a column raw does not have (typo or old column)
    SELECT array_agg(m.source_col) INTO stale
      FROM meta.etl_column_map m
     WHERE m.table_key = p_table_key
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns c
                        WHERE c.table_schema = cfg.raw_schema
                          AND c.table_name   = cfg.raw_table
                          AND c.column_name  = m.source_col);
    IF stale IS NOT NULL THEN
        RAISE WARNING 'build_final %: mapper has column(s) % that do not exist in % — ignored',
                      p_table_key, stale, raw;
    END IF;

    -- ── columns: raw order, mapper ordinal first ─────────────────────────────
    FOR rec IN
        SELECT c.column_name::text           AS src,
               coalesce(m.target_col, c.column_name::text) AS tgt,
               coalesce(m.target_type, 'text') AS typ,
               m.source_format               AS fmt
          FROM information_schema.columns c
          LEFT JOIN meta.etl_column_map m
                 ON m.table_key = p_table_key AND m.source_col = c.column_name
         WHERE c.table_schema = cfg.raw_schema
           AND c.table_name   = cfg.raw_table
           AND c.column_name NOT IN ('_row_hash', '_raw_id')
           AND coalesce(m.is_active, true)
         ORDER BY m.ordinal NULLS LAST, c.ordinal_position
    LOOP
        expr := CASE rec.typ
            WHEN 'text'      THEN format('r.%I', rec.src)
            WHEN 'integer'   THEN format('meta.try_integer(r.%I)', rec.src)
            WHEN 'bigint'    THEN format('meta.try_bigint(r.%I)', rec.src)
            WHEN 'numeric'   THEN format('meta.try_numeric(r.%I)', rec.src)
            WHEN 'boolean'   THEN format('meta.try_boolean(r.%I)', rec.src)
            WHEN 'date'      THEN format('meta.try_date(r.%I, %L)', rec.src, rec.fmt)
            WHEN 'timestamp' THEN format('meta.try_timestamp(r.%I, %L)', rec.src, rec.fmt)
        END;
        pg_type := CASE rec.typ WHEN 'timestamp' THEN 'timestamp without time zone'
                                ELSE rec.typ END;

        sel_items := sel_items || format('%s AS %I', expr, rec.tgt);
        col_defs  := col_defs  || format('%I %s', rec.tgt, pg_type);
        sig_items := sig_items || (rec.tgt || ' ' || pg_type);

        IF rec.typ <> 'text' THEN
            fail_conds := fail_conds || format(
                'count(*) FILTER (WHERE nullif(btrim(r.%I), '''') IS NOT NULL AND %s IS NULL)',
                rec.src, expr);
            fail_cols := fail_cols || rec.src;
        END IF;
    END LOOP;

    -- snapshot: version number + latest-version flag
    IF cfg.table_type = 'snapshot' THEN
        sel_items := sel_items || 'r._version AS "מספר_גרסה"'::text
                               || 'r._is_latest AS "גרסה_אחרונה"'::text;
        col_defs  := col_defs  || '"מספר_גרסה" bigint'::text
                               || '"גרסה_אחרונה" boolean'::text;
        sig_items := sig_items || 'מספר_גרסה bigint'::text
                               || 'גרסה_אחרונה boolean'::text;
    END IF;

    -- two columns ending up with the same final name
    SELECT array_agg(n) INTO dupes
      FROM (SELECT split_part(s, ' ', 1) AS n FROM unnest(sig_items) s) x
     GROUP BY n HAVING count(*) > 1;
    IF dupes IS NOT NULL THEN
        RAISE EXCEPTION 'build_final %: more than one column would be named % in the final table — fix meta.etl_column_map',
                        p_table_key, dupes;
    END IF;

    -- ── source rows: raw (+ versions) minus exclude_where ────────────────────
    IF cfg.table_type = 'snapshot' THEN
        key_cols := (SELECT string_agg(format('%I', k), ', ') FROM unnest(cfg.business_key) k);
        src_sql := format(
            '(SELECT x.*, '
            '        row_number() OVER (PARTITION BY %1$s ORDER BY x._raw_id) AS _version, '
            '        row_number() OVER (PARTITION BY %1$s ORDER BY x._raw_id DESC) = 1 AS _is_latest '
            '   FROM %2$s x) r', key_cols, raw);
    ELSE
        src_sql := format('%s r', raw);
    END IF;

    IF cfg.exclude_where IS NOT NULL AND btrim(cfg.exclude_where) <> '' THEN
        src_sql := src_sql || format(' WHERE NOT coalesce((%s), false)', cfg.exclude_where);
    END IF;

    -- ── create or refill the final table ─────────────────────────────────────
    want_sig := array_to_string(sig_items, ', ');

    IF to_regclass(fin) IS NOT NULL THEN
        SELECT string_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod), ', '
                          ORDER BY a.attnum)
          INTO have_sig
          FROM pg_attribute a
         WHERE a.attrelid = fin::regclass AND a.attnum > 0 AND NOT a.attisdropped;
    END IF;

    BEGIN
        IF have_sig IS NOT NULL AND have_sig = want_sig THEN
            EXECUTE format('TRUNCATE %s', fin);
        ELSE
            IF have_sig IS NOT NULL THEN
                BEGIN
                    EXECUTE format('DROP TABLE %s', fin);
                EXCEPTION WHEN dependent_objects_still_exist THEN
                    RAISE EXCEPTION 'build_final %: the columns of % changed, so it must be recreated, '
                                    'but a view depends on it. Drop the view, run again, then recreate it.',
                                    p_table_key, fin;
                END;
                RAISE NOTICE 'build_final %: columns changed — % was recreated (re-check permissions)',
                             p_table_key, fin;
            END IF;
            EXECUTE format('CREATE TABLE %s (%s)', fin, array_to_string(col_defs, ', '));
        END IF;

        EXECUTE format('INSERT INTO %s SELECT %s FROM %s',
                       fin, array_to_string(sel_items, ', '), src_sql);
        GET DIAGNOSTICS v_rows = ROW_COUNT;

        IF array_length(fail_conds, 1) > 0 THEN
            EXECUTE format('SELECT ARRAY[%s]::bigint[] FROM %s',
                           array_to_string(fail_conds, ', '), src_sql)
               INTO fail_counts;
        END IF;
    EXCEPTION WHEN syntax_error_or_access_rule_violation THEN
        RAISE EXCEPTION 'build_final %: SQL error: % — check exclude_where in meta.etl_tables '
                        '(it must use the ORIGINAL column names) and source_format in the mapper',
                        p_table_key, SQLERRM;
    END;

    IF fail_counts IS NOT NULL THEN
        FOR i IN 1 .. array_length(fail_counts, 1) LOOP
            IF fail_counts[i] > 0 THEN
                RAISE NOTICE 'build_final %: % value(s) in column % could not be converted and became NULL',
                             p_table_key, fail_counts[i], fail_cols[i];
                v_fail := v_fail + fail_counts[i];
            END IF;
        END LOOP;
    END IF;

    RAISE NOTICE 'build_final %: % rows written to %', p_table_key, v_rows, fin;

    final_rows    := v_rows;
    cast_failures := v_fail;
    RETURN NEXT;
END
$fn$;

COMMENT ON FUNCTION meta.build_final(text) IS
    'Step 3: rebuild the final (analyst) table from raw using the mapper and exclude_where.';
