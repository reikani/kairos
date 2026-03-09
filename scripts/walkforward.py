import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tuning"))

from scripts.forecast import Forecaster
from scripts.ingest import ingest_candles

SYMBOL = "X:BTCUSD"
INTERVAL = "15m"

# March 8th 2026 ET in UTC:
#   00:00 EST (UTC-5) = 05:00 UTC March 8
#   DST springs forward at 2am — 23 hour day
#   23:45 EDT (UTC-4) = 03:45 UTC March 9
#
# To get 32 forecasts per candle, start asof 8 hours before first target:
#   First target: 05:00 UTC March 8
#   Earliest asof: 05:00 - 32*15m = 21:00 UTC March 7
#   Latest asof: 03:30 UTC March 9

ASOF_START = pd.Timestamp("2026-03-07 21:00:00", tz="UTC")
ASOF_END = pd.Timestamp("2026-03-09 03:30:00", tz="UTC")
DELTA = pd.Timedelta(minutes=15)


def main():
    model_version = os.getenv("MODEL_VERSION", "XBTCUSD/15m/2025-09-01--2026-03-08")

    # Ingest candle data through March 9th (needed for later asof points)
    print("Ingesting March 8-9 candle data...")
    ingest_candles(SYMBOL, INTERVAL, "2026-03-08", "2026-03-09")

    forecaster = Forecaster(model_version)

    asof = ASOF_START
    total = int((ASOF_END - ASOF_START) / DELTA) + 1
    i = 0
    t_total = time.time()

    while asof <= ASOF_END:
        i += 1
        asof_str = asof.strftime("%Y-%m-%d %H:%M:%S+00")
        print(f"\n[{i}/{total}] asof={asof_str}")
        try:
            forecaster.forecast(asof_str, store=True)
        except AssertionError as e:
            print(f"  Skipped: {e}")
        asof += DELTA

    elapsed = time.time() - t_total
    print(f"\nDone: {i} forecasts in {elapsed:.0f}s ({elapsed/60:.1f}m)")


if __name__ == "__main__":
    main()
