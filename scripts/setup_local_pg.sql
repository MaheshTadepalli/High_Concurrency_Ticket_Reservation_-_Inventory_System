-- Optional: provision app role/db on a local PostgreSQL (run as superuser)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ticket') THEN
    CREATE ROLE ticket LOGIN PASSWORD 'ticket';
  END IF;
END
$$;

SELECT 'CREATE DATABASE tickets OWNER ticket'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tickets')\gexec

\c tickets
GRANT ALL ON SCHEMA public TO ticket;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ticket;
