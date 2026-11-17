-- =============================================================================
-- 004_clean_functions.sql
-- -----------------------------------------------------------------------------
-- Ready-made cleaning functions for meta.etl_column_map.transform_expr,
-- plus a small helper used by the check queries.
--
-- They run in step 3 (public -> analyst), on the text value, before the cast.
-- Raw in `public` always keeps the original value; only the analyst table sees
-- the cleaned one, so a change here is undone by a plain --final-only re-run.
--
-- Use one by writing {col} where the raw column goes:
--
--   UPDATE meta.etl_column_map
--      SET transform_expr = 'meta.digits_only({col})'
--    WHERE table_key = 'customer' AND source_col = 'person_id';
--
-- Any SQL expression is allowed, these are just the common ones:
--   'upper(btrim({col}))'
--   'replace({col}, ''-'', '''')'
--   'left(meta.digits_only({col}), 9)'
--
-- Add your own below; they live in `meta` so they are backed up and versioned
-- with the rest of the pipeline.
--
-- Safe to re-run (CREATE OR REPLACE).
-- =============================================================================


-- Keep only the digits 0-9.  '05-123/45 ' -> '0512345'
-- The usual fix for an id or a phone number the supplier sends with separators.
CREATE OR REPLACE FUNCTION meta.digits_only(v text) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT nullif(regexp_replace(coalesce(v, ''), '[^0-9]', '', 'g'), '') $$;

-- Keep only letters (any alphabet, Hebrew included), spaces and hyphens.
CREATE OR REPLACE FUNCTION meta.letters_only(v text) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT nullif(btrim(regexp_replace(coalesce(v, ''), '[^[:alpha:] \-]', '', 'g')), '') $$;

-- Trim, and collapse runs of whitespace into one space.  '  דנה   לוי ' -> 'דנה לוי'
CREATE OR REPLACE FUNCTION meta.squish(v text) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT nullif(btrim(regexp_replace(coalesce(v, ''), '\s+', ' ', 'g')), '') $$;

-- Empty or whitespace-only becomes NULL.
CREATE OR REPLACE FUNCTION meta.blank_to_null(v text) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT nullif(btrim(coalesce(v, '')), '') $$;

-- Drop leading zeros.  '000123' -> '123'
CREATE OR REPLACE FUNCTION meta.strip_leading_zeros(v text) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT nullif(regexp_replace(btrim(coalesce(v, '')), '^0+', ''), '') $$;

-- Pad on the left with zeros to `len` characters.  meta.zero_pad({col}, 9)
CREATE OR REPLACE FUNCTION meta.zero_pad(v text, len integer) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT CASE WHEN nullif(btrim(coalesce(v, '')), '') IS NULL THEN NULL
                ELSE lpad(btrim(v), len, '0') END $$;

-- Keep only the characters matching a POSIX class, e.g. meta.keep_chars({col}, '0-9A-Z')
CREATE OR REPLACE FUNCTION meta.keep_chars(v text, allowed text) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT nullif(regexp_replace(coalesce(v, ''), '[^' || allowed || ']', '', 'g'), '') $$;

-- Row count of a table that may not exist yet (NULL instead of an error).
-- Used by queries/show_objects.sql.
CREATE OR REPLACE FUNCTION meta.count_rows(p_schema text, p_table text) RETURNS bigint
LANGUAGE plpgsql STABLE AS $$
DECLARE n bigint;
BEGIN
    IF to_regclass(format('%I.%I', p_schema, p_table)) IS NULL THEN RETURN NULL; END IF;
    EXECUTE format('SELECT count(*) FROM %I.%I', p_schema, p_table) INTO n;
    RETURN n;
END $$;

-- Israeli phone: digits only, and 972... turned back into a leading 0.
CREATE OR REPLACE FUNCTION meta.il_phone(v text) RETURNS text
LANGUAGE sql IMMUTABLE AS
$$ SELECT CASE
            WHEN d IS NULL              THEN NULL
            WHEN d LIKE '972%'          THEN '0' || substr(d, 4)
            ELSE d
          END
     FROM (SELECT meta.digits_only(v) AS d) s $$;
