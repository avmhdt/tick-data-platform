-- Runs once, on the first start of an empty data directory.
-- M1 creates the database only; the tables (FR-11) arrive with the loader in M3.
CREATE DATABASE IF NOT EXISTS tickdb;
