UPSERT_CANDLES = """
INSERT INTO candles (
    symbol, interval, timestamp, open, high, low, close, volume, vwap
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, interval, timestamp) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    vwap = EXCLUDED.vwap;
"""

EXPORT_CANDLES_CSV = """
COPY (
    SELECT
        timestamp as timestamps,
        open,
        high,
        low,
        close,
        volume,
        volume * vwap as amount
    FROM candles
    WHERE
        symbol = %s
        AND interval = %s
        AND timestamp >= %s::timestamptz
        AND timestamp < %s::timestamptz
    ORDER BY timestamp ASC
) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)
"""

LOOKBACK_CANDLES = """
SELECT *
FROM (
    SELECT
        timestamp as timestamps,
        open,
        high,
        low,
        close,
        volume,
        volume * vwap as amount
    FROM candles
    WHERE
        symbol = %s
        AND interval = %s
        AND timestamp <= %s
    ORDER BY timestamp DESC
    LIMIT %s
)
ORDER BY timestamps ASC
"""

UPSERT_FORECASTS = """
INSERT INTO forecasts (
    symbol, interval, asof_timestamp, target_timestamp, model_version,
    open, high, low, close, volume, amount
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, interval, asof_timestamp, target_timestamp, model_version) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    amount = EXCLUDED.amount,
    generated_at = NOW();
"""
