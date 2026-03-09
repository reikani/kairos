CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,

    open  NUMERIC(20,10) NOT NULL,
    high  NUMERIC(20,10) NOT NULL,
    low   NUMERIC(20,10) NOT NULL,
    close NUMERIC(20,10) NOT NULL,

    volume NUMERIC(28,10) NOT NULL DEFAULT 0,
    vwap NUMERIC(20,10) NOT NULL DEFAULT 0,

    PRIMARY KEY (symbol, interval, timestamp)
);
