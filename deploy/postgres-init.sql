-- Runs once, on first start of an empty data volume. The gateway database is
-- reachable only inside the Compose network; the password is a local default.
CREATE USER gateway WITH PASSWORD 'gateway-local';
CREATE DATABASE gateway OWNER gateway;
REVOKE ALL ON DATABASE gateway FROM PUBLIC;
GRANT CONNECT ON DATABASE gateway TO gateway;
