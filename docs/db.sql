-- Run this script with user root. 
-- Please update password for users before running.

-- Create database room_service
CREATE DATABASE room_service WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';
ALTER DATABASE room_service OWNER TO root;

-- Connect to database room_service
\c room_service

-- Revoke privileges from 'public' role
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE room_service FROM PUBLIC;
REVOKE ALL ON schema public FROM PUBLIC;

-- Read-only role for database room_service
CREATE ROLE role_room_service_ro;
GRANT CONNECT ON DATABASE room_service TO role_room_service_ro;
GRANT USAGE ON SCHEMA public TO role_room_service_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_room_service_ro;
-- Grant selelect to role_room_service_ro for tables in public schema created in the future
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO role_room_service_ro;

-- Read/write role for database room_service
CREATE ROLE role_room_service_rw;
GRANT CONNECT ON DATABASE room_room TO role_room_service_rw;
GRANT USAGE ON SCHEMA public TO role_room_service_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO role_room_service_rw;
-- Grant select, insert, update, delete to role_room_service_rw for tables in public schema created in the future
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO role_room_service_rw;
-- Grant the use of the currval and nextval functions for sequences.
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO role_room_service_rw;
-- Grant usage to role_room_service_rw for sequences in public schema created in the future
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO role_room_service_rw;

-- Users creation
CREATE USER user_room_service_ro WITH PASSWORD 'some_random_passwd';
CREATE USER user_room_service_rw WITH PASSWORD 'some_random_passwd';

-- Grant privileges to users
GRANT role_room_service_ro TO user_room_service_ro;
GRANT role_room_service_rw TO user_room_service_rw;

-- Create tables, indexes. From pg_dump
--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;

--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

CREATE TABLE public.room_data (
    key character varying NOT NULL,
    type character varying NOT NULL,
    value character varying NOT NULL,
    deleted boolean NOT NULL DEFAULT false,
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    synced_at timestamp with time zone NOT NULL DEFAULT now(),
    expire_at timestamp with time zone,
    version bigint NOT NULL DEFAULT 0,
);

ALTER TABLE ONLY public.room_data
    ADD CONSTRAINT room_data_pkey PRIMARY KEY (key);

