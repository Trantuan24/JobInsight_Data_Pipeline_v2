-- ============================================
-- JobInsight Database Initialization
-- ============================================

-- Create jobinsight user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'jobinsight') THEN
        CREATE USER jobinsight WITH PASSWORD 'jobinsight';
    END IF;
END
$$;

ALTER ROLE jobinsight WITH LOGIN CREATEDB;

-- Create jobinsight database
CREATE DATABASE jobinsight OWNER jobinsight;

-- Connect to jobinsight database
\c jobinsight;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE jobinsight TO jobinsight;

-- ============================================
-- EXTENSIONS
-- ============================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- SCHEMAS
-- ============================================

ALTER SCHEMA public OWNER TO jobinsight;
GRANT ALL ON SCHEMA public TO jobinsight;

CREATE SCHEMA IF NOT EXISTS jobinsight_staging AUTHORIZATION jobinsight;
GRANT ALL ON SCHEMA jobinsight_staging TO jobinsight;

CREATE SCHEMA IF NOT EXISTS monitoring AUTHORIZATION jobinsight;
GRANT ALL ON SCHEMA monitoring TO jobinsight;

\echo '✅ JobInsight database initialization completed'
