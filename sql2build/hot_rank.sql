-- stock_hot_rank
CREATE TABLE public.stock_hot_rank (
    id SERIAL PRIMARY KEY,
    stock_code character varying NOT NULL,
    rank integer,
    new_fans_ratio double precision,
    loyal_fans_ratio double precision,
    date date NOT NULL
);

ALTER TABLE public.stock_hot_rank OWNER TO si;

-- 创建复合索引以加速查询
CREATE INDEX idx_stock_hot_rank_stock_code_date ON public.stock_hot_rank (stock_code, date);

-- 添加唯一约束，确保每个股票每天只有一条记录
ALTER TABLE ONLY public.stock_hot_rank
    ADD CONSTRAINT stock_hot_rank_stock_code_date_key UNIQUE (stock_code, date);