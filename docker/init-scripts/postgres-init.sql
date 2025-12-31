-- ============================================
-- JobInsight PostgreSQL Initialization Script (Updating)
-- ============================================

\echo ''
\echo '╔════════════════════════════════════════════════════════════╗'
\echo '║  JobInsight Data Pipeline - Database Initialization       ║'
\echo '╚════════════════════════════════════════════════════════════╝'
\echo ''

\echo '📌 Step 1: Creating database and users...'
\ir /docker-entrypoint-initdb.d/schemas/init.sql

\echo ''
\echo '📌 Step 2: Creating raw schema...'
\ir /docker-entrypoint-initdb.d/schemas/raw_schema.sql

\echo ''
\echo '📌 Step 3: Creating staging schema...'
\ir /docker-entrypoint-initdb.d/schemas/staging_schema.sql

\echo ''
\echo '📌 Step 4: Creating monitoring schema...'
\ir /docker-entrypoint-initdb.d/schemas/monitoring_schema.sql

\echo ''
\echo '📌 Step 5: Creating stored procedures...'
\ir /docker-entrypoint-initdb.d/procedures/staging_procedures.sql

\echo ''
\echo '╔════════════════════════════════════════════════════════════╗'
\echo '║  ✅ PostgreSQL Initialization Completed                    ║'
\echo '║  Database: jobinsight | User: jobinsight                   ║'
\echo '║  Schemas: public, jobinsight_staging, monitoring           ║'
\echo '╚════════════════════════════════════════════════════════════╝'
\echo ''
