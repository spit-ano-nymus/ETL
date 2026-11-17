-- =============================================================================
-- Start the payments practice over: drops its data, keeps meta and the config.
--
--   python main.py --pipeline tests/local/payments_local.yaml --sql tests/local/reset_payments.sql
--
-- To clear the config too, just re-apply payments_setup.sql (it starts with a
-- DELETE). To make the Python function run again, clear its cache as well.
-- =============================================================================

DROP TABLE IF EXISTS staging.payments;
DROP TABLE IF EXISTS public.payments;
DROP TABLE IF EXISTS analyst."תשלומים";

DO $$
DECLARE t text;
BEGIN
    FOR t IN SELECT snapshot_table FROM meta.staging_snapshots WHERE table_key = 'payments' LOOP
        EXECUTE format('DROP TABLE IF EXISTS staging.%I', t);
    END LOOP;
END $$;

DELETE FROM meta.staging_snapshots WHERE table_key = 'payments';
DELETE FROM meta.etl_runs WHERE table_key = 'payments';
DELETE FROM meta.py_func_cache WHERE func_name IN ('strip_nikud', 'normalize_space_py');
