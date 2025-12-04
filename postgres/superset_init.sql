-- Superset metadata DB/user init script (safe to run multiple times)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset') THEN
        CREATE DATABASE superset;
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset_user') THEN
        CREATE USER superset_user WITH PASSWORD 'superset_pass';
    END IF;
END$$;

GRANT ALL PRIVILEGES ON DATABASE superset TO superset_user;

CREATE USER example WITH PASSWORD 'example';
CREATE DATABASE example_db OWNER example;
GRANT ALL PRIVILEGES ON DATABASE example_db TO example;