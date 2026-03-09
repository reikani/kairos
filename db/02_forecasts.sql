CREATE TABLE IF NOT EXISTS forecasts (
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    asof_timestamp TIMESTAMPTZ NOT NULL,
    target_timestamp TIMESTAMPTZ NOT NULL,
    model_version TEXT NOT NULL,

    open  NUMERIC(20,10) NOT NULL,
    high  NUMERIC(20,10) NOT NULL,
    low   NUMERIC(20,10) NOT NULL,
    close NUMERIC(20,10) NOT NULL,

    volume NUMERIC(28,10) NOT NULL,
    amount NUMERIC(28,10) NOT NULL,

    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, interval, asof_timestamp, target_timestamp, model_version)
);
