import sys

sys.path.append("../")

import os
import re
import time
from datetime import date, datetime, timedelta, timezone

from massive import RESTClient

from db import conn
from db.queries import UPSERT_CANDLES

client = RESTClient(os.getenv("MASSIVE_API_KEY"))

TIMESPAN_MAP = {"m": "minute", "h": "hour", "d": "day", "w": "week"}


def parse_interval(interval):
    match = re.match(r"(\d+)([mhdw])", interval)
    if not match:
        raise ValueError(f"Invalid interval: {interval}")
    return int(match.group(1)), TIMESPAN_MAP[match.group(2)]


def _month_ranges(start_str, end_str):
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    cursor = start
    while cursor <= end:
        next_month = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        yield cursor.isoformat(), chunk_end.isoformat()
        cursor = next_month


def ingest_candles(symbol, interval, start, end):
    multiplier, timespan = parse_interval(interval)

    for chunk_start, chunk_end in _month_ranges(start, end):
        rows = [
            (
                symbol,
                interval,
                datetime.fromtimestamp(a.timestamp / 1000, tz=timezone.utc),
                a.open,
                a.high,
                a.low,
                a.close,
                float(a.volume),
                float(a.vwap),
            )
            for a in client.list_aggs(
                symbol,
                multiplier,
                timespan,
                chunk_start,
                chunk_end,
                adjusted=True,
                sort="asc",
                limit=50_000,
            )
        ]

        with conn() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(UPSERT_CANDLES, rows)
            connection.commit()

        print(
            f"Upserted {len(rows)} {interval} candles for {symbol} ({chunk_start} to {chunk_end})"
        )
        time.sleep(1)


if __name__ == "__main__":
    ingest_candles(
        os.getenv("SYMBOL", "X:BTCUSD"),
        os.getenv("INTERVAL", "15m"),
        os.getenv("START", "2025-09-01"),
        os.getenv("END", datetime.now().strftime("%Y-%m-%d")),
    )
