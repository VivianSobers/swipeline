-- Creates metadata databases and users for Druid and Superset.
CREATE USER druid WITH PASSWORD 'druid-local';
CREATE DATABASE druid OWNER druid;
REVOKE ALL ON DATABASE druid FROM PUBLIC;
GRANT CONNECT ON DATABASE druid TO druid;

CREATE USER superset WITH PASSWORD 'superset-local';
CREATE DATABASE superset OWNER superset;
REVOKE ALL ON DATABASE superset FROM PUBLIC;
GRANT CONNECT ON DATABASE superset TO superset;
