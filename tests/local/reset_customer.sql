-- =============================================================================
-- Start the customer test over (data only — meta tables and the config stay).
-- =============================================================================
DROP TABLE IF EXISTS staging.customer;
DROP TABLE IF EXISTS public.customer;
DROP TABLE IF EXISTS analyst."לקוחות";
DELETE FROM meta.etl_runs WHERE table_key = 'customer';
