import json
import urllib.request
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from db import conn

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"

# March 8th ET in UTC: first close 05:00, last close 04:00 March 9
MIN_CLOSE_TS = int(datetime(2026, 3, 8, 5, 0, tzinfo=timezone.utc).timestamp())
MAX_CLOSE_TS = int(datetime(2026, 3, 9, 4, 1, tzinfo=timezone.utc).timestamp())

FORECASTS_FOR_TARGET = """
SELECT asof_timestamp, close
FROM forecasts
WHERE symbol = 'X:BTCUSD' AND interval = '15m'
  AND target_timestamp = %s
  AND model_version = %s
ORDER BY asof_timestamp ASC
"""


def fetch_markets():
    url = (
        f"{KALSHI_BASE}/markets?series_ticker={SERIES}&status=settled"
        f"&limit=200&min_close_ts={MIN_CLOSE_TS}&max_close_ts={MAX_CLOSE_TS}"
    )
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read())
    # Sort by open_time ascending (API returns newest first)
    markets = sorted(data["markets"], key=lambda m: m["open_time"])
    return markets


def ema_weighted_close(forecasts_df, alpha=0.15):
    """EMA-weight forecast closes. Most recent asof gets highest weight."""
    n = len(forecasts_df)
    if n == 0:
        return None
    # weights: newest (last row) gets highest weight
    # w_i = alpha * (1 - alpha)^(n - 1 - i) for i in 0..n-1
    weights = np.array([alpha * (1 - alpha) ** (n - 1 - i) for i in range(n)])
    weights /= weights.sum()
    return (forecasts_df["close"].values * weights).sum()


def main():
    model_version = "XBTCUSD/15m/2025-09-01--2026-03-08"

    print("Fetching Kalshi KXBTC15M markets for March 8th ET...")
    markets = fetch_markets()
    print(f"Found {len(markets)} markets\n")

    hits, misses, skipped = 0, 0, 0
    results = []

    with conn() as connection:
        with connection.cursor() as cursor:
            for m in markets:
                ticker = m["ticker"]
                open_time = m["open_time"]  # UTC ISO8601
                if not m.get("floor_strike") or not m.get("expiration_value"):
                    skipped += 1
                    continue
                strike = float(m["floor_strike"])
                actual = m["result"]  # "yes" or "no"
                expiry_val = float(m["expiration_value"])

                # Map Kalshi open_time to our candle timestamp
                candle_ts = datetime.fromisoformat(open_time.replace("Z", "+00:00"))

                cursor.execute(FORECASTS_FOR_TARGET, (candle_ts, model_version))
                rows = cursor.fetchall()

                if not rows:
                    skipped += 1
                    continue

                fc_df = pd.DataFrame(rows, columns=["asof_timestamp", "close"])
                fc_df["close"] = fc_df["close"].astype(float)
                pred_close = ema_weighted_close(fc_df)

                predicted = "yes" if pred_close >= strike else "no"
                hit = predicted == actual

                if hit:
                    hits += 1
                else:
                    misses += 1

                # ET label for display
                et_label = ticker.split("-")[1]  # e.g., 26MAR080015

                mark = "HIT " if hit else "MISS"
                results.append(
                    {
                        "ticker": ticker,
                        "et": et_label,
                        "strike": strike,
                        "pred_close": pred_close,
                        "expiry_val": expiry_val,
                        "actual": actual,
                        "predicted": predicted,
                        "hit": hit,
                        "n_forecasts": len(rows),
                    }
                )

                print(
                    f"  {mark}  {et_label}  "
                    f"strike=${strike:,.2f}  pred=${pred_close:,.2f}  "
                    f"actual=${expiry_val:,.2f}  "
                    f"({'YES' if actual == 'yes' else 'NO':>3})"
                )

    total = hits + misses
    print(f"\n{'='*60}")
    print(f"Results: {hits}/{total} HITs ({100*hits/total:.1f}%)")
    print(f"         {misses}/{total} MISSes ({100*misses/total:.1f}%)")
    if skipped:
        print(f"         {skipped} skipped (no forecasts)")


if __name__ == "__main__":
    main()
