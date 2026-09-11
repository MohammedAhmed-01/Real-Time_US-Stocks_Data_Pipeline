-- =============================================================================
-- init_postgres.sql
-- PostgreSQL database initialisation for the US Stocks Analytics pipeline.
--
-- Mounted to: /docker-entrypoint-initdb.d/init.sql
-- Runs automatically when the postgres container starts for the FIRST time.
--
-- What this file does:
--   • Enables useful extensions
--   • Grants the application user full privileges on the public schema
--   • Leaves table creation to Spark JDBC (analytics_job.py)
-- =============================================================================

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;  -- query performance tracking
CREATE EXTENSION IF NOT EXISTS btree_gin;           -- composite GIN index support

-- ── Schema grants ─────────────────────────────────────────────────────────────
GRANT ALL ON SCHEMA public TO stocks;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES    TO stocks;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO stocks;

-- ── Database comment ─────────────────────────────────────────────────────────
COMMENT ON DATABASE stocks_analytics IS
    'US Stocks real-time pipeline — SparkSQL analytics output (M3)';
