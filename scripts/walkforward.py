import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tuning"))

from scripts.forecast import Forecaster

SYMBOL = "X:BTCUSD"
INTERVAL = "15m"
DELTA = pd.Timedelta(minutes=15)


def walkforward(date_str, utc_offset=-5):
    """Run forecasts for all candles on a given ET date.

    Args:
        date_str: Date in YYYY-MM-DD format (ET)
        utc_offset: -5 for EST, -4 for EDT
    """
    model_version = os.getenv("MODEL_VERSION", "XBTCUSD/15m/2025-09-01--2026-03-08")

    # First target candle: midnight ET → UTC
    first_target = pd.Timestamp(f"{date_str} 00:00:00", tz="UTC") - pd.Timedelta(
        hours=utc_offset
    )
    # Last target: 23:45 ET → UTC
    last_target = pd.Timestamp(f"{date_str} 23:45:00", tz="UTC") - pd.Timedelta(
        hours=utc_offset
    )

    # Asof range: 32 steps before first target to 1 step before last target
    asof_start = first_target - 32 * DELTA
    asof_end = last_target - DELTA

    print(f"Walk-forward for {date_str} ET (UTC offset {utc_offset})")
    print(f"  Targets: {first_target} to {last_target}")
    print(f"  Asof:    {asof_start} to {asof_end}")

    forecaster = Forecaster(model_version)

    asof = asof_start
    total = int((asof_end - asof_start) / DELTA) + 1
    i = 0
    t_total = time.time()

    while asof <= asof_end:
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
    # Usage: python -m scripts.walkforward 2026-03-07
    #        python -m scripts.walkforward 2026-03-07 -5
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-03-08"
    offset = int(sys.argv[2]) if len(sys.argv) > 2 else -5
    walkforward(date, offset)
