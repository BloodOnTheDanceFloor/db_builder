-- daily_index
CREATE TABLE public.daily_index (
    symbol character varying NOT NULL,
    date date NOT NULL,
    open double precision,
    close double precision,
    high double precision,
    low double precision,
    volume bigint,
    amount bigint,
    amplitude double precision,
    change_rate double precision,
    change_amount double precision,
    turnover_rate double precision
);


ALTER TABLE public.daily_index OWNER TO si;

--
-- Name: daily_index daily_index_pkey; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.daily_index
    ADD CONSTRAINT daily_index_pkey PRIMARY KEY (symbol, date);


--
-- Name: daily_index delete_derived_index_on_delete; Type: TRIGGER; Schema: public; Owner: si
--

CREATE TRIGGER delete_derived_index_on_delete AFTER DELETE ON public.daily_index FOR EACH ROW EXECUTE FUNCTION public.delete_derived_on_delete();


--
-- Name: daily_index update_derived_index_on_change; Type: TRIGGER; Schema: public; Owner: si
--

CREATE TRIGGER update_derived_index_on_change AFTER INSERT OR UPDATE ON public.daily_index FOR EACH ROW EXECUTE FUNCTION public.update_derived_on_change();


-- index_info
CREATE TABLE public.index_info (
    symbol character varying NOT NULL,
    name character varying(100) NOT NULL
);


ALTER TABLE public.index_info OWNER TO si;

--
-- Name: index_info index_info_pkey; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.index_info
    ADD CONSTRAINT index_info_pkey PRIMARY KEY (symbol);


-- derived_index
CREATE TABLE public.derived_index (
    symbol character varying(10) NOT NULL,
    date date NOT NULL,
    real_change double precision
);


ALTER TABLE public.derived_index OWNER TO si;

--
-- Name: derived_index derived_index_pk; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.derived_index
    ADD CONSTRAINT derived_index_pk PRIMARY KEY (symbol, date);