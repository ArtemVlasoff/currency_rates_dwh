CREATE DATABASE metabase;

CREATE USER metabase_user WITH PASSWORD '0000';

GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase_user;

GRANT ALL ON SCHEMA public TO metabase_user;