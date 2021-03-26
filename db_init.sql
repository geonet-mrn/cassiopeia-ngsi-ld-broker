--
-- PostgreSQL database dump
--

-- Dumped from database version 12.6 (Ubuntu 12.6-0ubuntu0.20.04.1)
-- Dumped by pg_dump version 12.6 (Ubuntu 12.6-0ubuntu0.20.04.1)

-- Started on 2021-03-25 16:39:05 CET

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
-- TOC entry 2 (class 3079 OID 37814)
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- TOC entry 3854 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 208 (class 1259 OID 38816)
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
-- TOC entry 209 (class 1259 OID 38822)
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
-- TOC entry 3855 (class 0 OID 0)
-- Dependencies: 209
-- Name: attributes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.attributes_id_seq OWNED BY public.attributes.instance_id;


--
-- TOC entry 210 (class 1259 OID 38824)
-- Name: entities; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.entities (
    id bigint NOT NULL,
    ent_id character varying NOT NULL,
    ent_type character varying NOT NULL,
    ent_created_at timestamp without time zone NOT NULL,
    ent_modified_at timestamp without time zone NOT NULL,
    ent_temporal boolean NOT NULL
);


ALTER TABLE public.entities OWNER TO postgres;

--
-- TOC entry 211 (class 1259 OID 38830)
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
-- TOC entry 3856 (class 0 OID 0)
-- Dependencies: 211
-- Name: entities_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.entities_id_seq OWNED BY public.entities.id;


--
-- TOC entry 3704 (class 2604 OID 38832)
-- Name: attributes instance_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.attributes ALTER COLUMN instance_id SET DEFAULT nextval('public.attributes_id_seq'::regclass);


--
-- TOC entry 3705 (class 2604 OID 38833)
-- Name: entities id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.entities ALTER COLUMN id SET DEFAULT nextval('public.entities_id_seq'::regclass);


--
-- TOC entry 3845 (class 0 OID 38816)
-- Dependencies: 208
-- Data for Name: attributes; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.attributes (instance_id, eid, attr_name, dataset_id, json, geom, attr_type, attr_created_at, attr_modified_at, attr_observed_at) FROM stdin;
\.


--
-- TOC entry 3847 (class 0 OID 38824)
-- Dependencies: 210
-- Data for Name: entities; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.entities (id, ent_id, ent_type, ent_created_at, ent_modified_at, ent_temporal) FROM stdin;
\.


--
-- TOC entry 3702 (class 0 OID 38119)
-- Dependencies: 204
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) FROM stdin;
\.


--
-- TOC entry 3857 (class 0 OID 0)
-- Dependencies: 209
-- Name: attributes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.attributes_id_seq', 1, false);


--
-- TOC entry 3858 (class 0 OID 0)
-- Dependencies: 211
-- Name: entities_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.entities_id_seq', 15, true);


--
-- TOC entry 3709 (class 2606 OID 38835)
-- Name: attributes attributes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.attributes
    ADD CONSTRAINT attributes_pkey PRIMARY KEY (instance_id);


--
-- TOC entry 3711 (class 2606 OID 38837)
-- Name: entities ent_id_and_temporal_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT ent_id_and_temporal_unique UNIQUE (ent_id, ent_temporal);


--
-- TOC entry 3713 (class 2606 OID 38839)
-- Name: entities entities_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);


-- Completed on 2021-03-25 16:39:05 CET

--
-- PostgreSQL database dump complete
--

