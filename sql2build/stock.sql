-- daily_stock
CREATE TABLE public.daily_stock (
    symbol character varying NOT NULL,
    date date NOT NULL,
    open double precision,
    close double precision,
    high double precision,
    low double precision,
    volume bigint,
    amount bigint,
    outstanding_share double precision,
    turnover double precision
);


ALTER TABLE public.daily_stock OWNER TO si;

--
-- Name: daily_stock daily_stock_pkey; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.daily_stock
    ADD CONSTRAINT daily_stock_pkey PRIMARY KEY (symbol, date);


--
-- Name: daily_stock delete_derived_stock_on_delete; Type: TRIGGER; Schema: public; Owner: si
--

CREATE TRIGGER delete_derived_stock_on_delete AFTER DELETE ON public.daily_stock FOR EACH ROW EXECUTE FUNCTION public.delete_derived_on_delete();


--
-- Name: daily_stock update_derived_stock_on_change; Type: TRIGGER; Schema: public; Owner: si
--

CREATE TRIGGER update_derived_stock_on_change AFTER INSERT OR UPDATE ON public.daily_stock FOR EACH ROW EXECUTE FUNCTION public.update_derived_on_change();

-- stock_info
CREATE TABLE public.stock_info (
    symbol character varying NOT NULL,
    name character varying(100) NOT NULL,
    index_2020 character(6),
    index_2021 character(6),
    index_2022 character(6),
    index_2023 character(6),
    index_2024 character(6),
    index_2025 character varying(10),
    index_2026 character varying(10)
);


ALTER TABLE public.stock_info OWNER TO si;

--
-- Name: stock_info stock_info_pkey; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.stock_info
    ADD CONSTRAINT stock_info_pkey PRIMARY KEY (symbol);

-- derived_stock
CREATE TABLE public.derived_stock (
    symbol character varying(10) NOT NULL,
    date date NOT NULL,
    real_change double precision
);


ALTER TABLE public.derived_stock OWNER TO si;

--
-- Name: derived_stock derived_stock_pk; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.derived_stock
    ADD CONSTRAINT derived_stock_pk PRIMARY KEY (symbol, date);

-- stock_hot_rank
CREATE TABLE public.stock_hot_rank (
    id integer NOT NULL,
    symbol character varying NOT NULL,
    rank integer NOT NULL,
    new_fans_ratio double precision,
    loyal_fans_ratio double precision,
    date timestamp without time zone NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);


ALTER TABLE public.stock_hot_rank OWNER TO si;

--
-- Name: stock_hot_rank stock_hot_rank_pkey; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.stock_hot_rank
    ADD CONSTRAINT stock_hot_rank_pkey PRIMARY KEY (id);

--
-- Name: ix_stock_hot_rank_id; Type: INDEX; Schema: public; Owner: si
--

CREATE INDEX ix_stock_hot_rank_id ON public.stock_hot_rank USING btree (id);

--
-- Name: ix_stock_hot_rank_stock_code; Type: INDEX; Schema: public; Owner: si
--

CREATE INDEX ix_stock_hot_rank_stock_code ON public.stock_hot_rank USING btree (symbol);