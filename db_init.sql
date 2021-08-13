--
-- PostgreSQL database dump
--

-- Dumped from database version 12.8 (Ubuntu 12.8-0ubuntu0.20.04.1)
-- Dumped by pg_dump version 12.8 (Ubuntu 12.8-0ubuntu0.20.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: attributes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.attributes (
    instance_id bigint NOT NULL,
    eid bigint NOT NULL,
    attr_name character varying NOT NULL,
    dataset_id character varying,
    json jsonb,
    geom public.geometry,
    attr_type smallint NOT NULL,
    attr_created_at timestamp without time zone NOT NULL,
    attr_modified_at timestamp without time zone NOT NULL,
    attr_observed_at timestamp without time zone
);


ALTER TABLE public.attributes OWNER TO postgres;

--
-- Name: attributes_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.attributes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.attributes_id_seq OWNER TO postgres;

--
-- Name: attributes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.attributes_id_seq OWNED BY public.attributes.instance_id;


--
-- Name: entities; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.entities (
    id bigint NOT NULL,
    ent_id character varying NOT NULL,
    ent_type character varying NOT NULL,
    ent_created_at timestamp without time zone NOT NULL,
    ent_modified_at timestamp without time zone NOT NULL
);


ALTER TABLE public.entities OWNER TO postgres;

--
-- Name: entities_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.entities_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.entities_id_seq OWNER TO postgres;

--
-- Name: entities_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.entities_id_seq OWNED BY public.entities.id;


--
-- Name: latest_attributes; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.latest_attributes AS
 SELECT x.r,
    x.instance_id,
    x.eid,
    x.attr_name,
    x.dataset_id,
    x.json,
    x.geom,
    x.attr_type,
    x.attr_created_at,
    x.attr_modified_at,
    x.attr_observed_at
   FROM ( SELECT row_number() OVER (PARTITION BY t.eid, t.attr_name, t.dataset_id ORDER BY t.instance_id DESC) AS r,
            t.instance_id,
            t.eid,
            t.attr_name,
            t.dataset_id,
            t.json,
            t.geom,
            t.attr_type,
            t.attr_created_at,
            t.attr_modified_at,
            t.attr_observed_at
           FROM public.attributes t) x
  WHERE (x.r <= 1);


ALTER TABLE public.latest_attributes OWNER TO postgres;

--
-- Name: attributes instance_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.attributes ALTER COLUMN instance_id SET DEFAULT nextval('public.attributes_id_seq'::regclass);


--
-- Name: entities id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.entities ALTER COLUMN id SET DEFAULT nextval('public.entities_id_seq'::regclass);


--
-- Data for Name: attributes; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.attributes (instance_id, eid, attr_name, dataset_id, json, geom, attr_type, attr_created_at, attr_modified_at, attr_observed_at) FROM stdin;
\.


--
-- Data for Name: entities; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.entities (id, ent_id, ent_type, ent_created_at, ent_modified_at) FROM stdin;
\.


--
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) FROM stdin;
\.


--
-- Name: attributes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.attributes_id_seq', 1, false);


--
-- Name: entities_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.entities_id_seq', 15, true);


--
-- Name: attributes attributes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.attributes
    ADD CONSTRAINT attributes_pkey PRIMARY KEY (instance_id);


--
-- Name: entities ent_id_and_temporal_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT ent_id_and_temporal_unique UNIQUE (ent_id);


--
-- Name: entities entities_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

