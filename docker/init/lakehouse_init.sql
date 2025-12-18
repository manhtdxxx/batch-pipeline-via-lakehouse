CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;


CREATE TABLE IF NOT EXISTS iceberg.silver.company (
    symbol VARCHAR,
    company_name VARCHAR,
    icb_code_1 VARCHAR,
    icb_code_2 VARCHAR,
    icb_code_3 VARCHAR,
    icb_code_4 VARCHAR,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);
-- WITH (
--     format = 'PARQUET',
--     partitioning = ARRAY['ingest_year', 'ingest_month']
-- );


CREATE TABLE IF NOT EXISTS iceberg.silver.industry (
    icb_code VARCHAR,
    level INT,
    icb_name VARCHAR,
    en_icb_name VARCHAR,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.silver.company_shareholders (
    id VARCHAR,
    shareholder_name VARCHAR,
    symbol VARCHAR,
    quantity BIGINT,
    ownership_percentage DOUBLE,
    updated_at DATE,
    is_active BOOLEAN,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.silver.company_events (
    id VARCHAR,
    event_code VARCHAR,
    event_name VARCHAR,
    en_event_name VARCHAR,
    symbol VARCHAR,
    issue_date DATE,
    ratio DOUBLE,
    value BIGINT,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.silver.quarterly_ratio (
    symbol VARCHAR,
    year INT,
    quarter INT,
    roa DOUBLE,
    roe DOUBLE,
    net_profit_margin DOUBLE,
    market_capital DOUBLE,
    outstanding_share BIGINT,
    price_to_earnings DOUBLE,
    price_to_book_value DOUBLE,
    price_to_sales DOUBLE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.silver.daily_ohlcv (
    symbol VARCHAR,
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.gold.fact_quarterly_ratio (
    id VARCHAR,
    symbol VARCHAR,
    date DATE,
    -- original ratio columns
    roa DOUBLE,
    roe DOUBLE,
    net_profit_margin DOUBLE,
    market_capital DOUBLE,
    outstanding_share BIGINT,
    price_to_earnings DOUBLE,
    price_to_book_value DOUBLE,
    price_to_sales DOUBLE,
    -- derived columns
    price DOUBLE,
    eps DOUBLE,
    bvps DOUBLE,
    revenue DOUBLE,
    net_profit DOUBLE,
    asset DOUBLE, 
    equity DOUBLE,
    debt DOUBLE,
    debt_to_asset DOUBLE,
    financial_leverage DOUBLE,
    asset_turnover DOUBLE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.gold.fact_cash_dividend (
    id VARCHAR,
    symbol VARCHAR,
    issue_date DATE,
    ratio DOUBLE,
    value BIGINT,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.gold.fact_share_issue (
    id VARCHAR,
    symbol VARCHAR,
    issue_date DATE,
    ratio DOUBLE,
    approx_share_before_issue BIGINT,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.gold.dim_date (
    date DATE,
    year INT,
    quarter_num INT,
    quarter VARCHAR,
    month_num INT,
    month VARCHAR,
    weekday_num INT,
    weekday VARCHAR,
    is_weekend BOOLEAN
);


CREATE TABLE IF NOT EXISTS iceberg.gold.dim_company (
    symbol VARCHAR,
    company_name VARCHAR,
    icb_code VARCHAR,
    icb_name VARCHAR,
    en_icb_name VARCHAR,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.gold.dim_shareholder (
    id VARCHAR,
    shareholder_name VARCHAR,
    symbol VARCHAR,
    quantity BIGINT,
    ownership_percentage DOUBLE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS iceberg.gold.fact_daily_ohlcv (
    id VARCHAR,
    symbol VARCHAR,
    date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    -- Return
    return DOUBLE,
    -- Candle metrics
    range DOUBLE,
    body DOUBLE,
    upper_wick DOUBLE,
    lower_wick DOUBLE,
    body_ratio DOUBLE,
    upper_ratio DOUBLE,
    lower_ratio DOUBLE,
    is_green INT,
    -- EMA
    ema_10 DOUBLE,
    ema_20 DOUBLE,
    ema_10_dist_pct DOUBLE,
    ema_20_dist_pct DOUBLE,
    -- RSI
    rsi_14 DOUBLE,
    rsi_14_scaled DOUBLE,
    -- Volume metrics
    vol_ema_10 BIGINT,
    rvol_10 DOUBLE,
    -- Labels
    pct_change_std DOUBLE,
    label_1 INT,
    label_2 INT,
    label_3 INT,
    ingest_timestamp TIMESTAMP WITH TIME ZONE
);
