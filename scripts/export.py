import os

from db import conn
from db.queries import EXPORT_CANDLES_CSV
from scripts.ingest import ingest_candles

SYMBOL = os.getenv("SYMBOL", "X:BTCUSD")
INTERVAL = os.getenv("INTERVAL", "15m")
START = os.getenv("START", "2025-09-01")
END = os.getenv("END", "2026-03-08")

# Ingest from Massive
ingest_candles(SYMBOL, INTERVAL, START, END)

# Export to CSV
with conn() as conn:
    with (
        conn.cursor().copy(EXPORT_CANDLES_CSV, (SYMBOL, INTERVAL, START, END)) as copy,
        open("data.csv", "wb") as f,
    ):
        for chunk in copy:
            f.write(chunk)
print(f"Exported {INTERVAL} data for {SYMBOL} from {START} to {END}")
