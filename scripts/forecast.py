import os
import sys
import time

import pandas as pd
import torch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tuning"))
from model import Kronos, KronosTokenizer, KronosPredictor

from db import conn
from db.queries import LOOKBACK_CANDLES, UPSERT_FORECASTS

LOOKBACK = 160
PREDICT = 32


def get_device():
    if torch.cuda.is_available():
        return "cuda"
    elif torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def next_candle_timestamps(ts, n, interval_minutes=15):
    """Generate next n candle timestamps after ts, spaced by interval_minutes. BTC is 24/7."""
    start = pd.Timestamp(ts)
    if start.tzinfo is None:
        start = start.tz_localize("UTC")
    delta = pd.Timedelta(minutes=interval_minutes)
    return [start + delta * (i + 1) for i in range(n)]


class Forecaster:
    def __init__(self, model_version: str):
        self.model_version = model_version
        self.device = get_device()

        self.predictor = KronosPredictor(
            Kronos.from_pretrained(f"./tuned/{model_version}/basemodel/best_model"),
            KronosTokenizer.from_pretrained(
                f"./tuned/{model_version}/tokenizer/best_model"
            ),
            device=self.device,
        )
        print(f"Model loaded on {self.device}")

    def forecast(self, asof_utc: str, store: bool = True):
        """Run a single forecast from asof_utc timestamp. Returns predicted OHLCV DataFrame."""
        with conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    LOOKBACK_CANDLES, ("X:BTCUSD", "15m", asof_utc, LOOKBACK)
                )
                rows = cursor.fetchall()
                assert (
                    len(rows) == LOOKBACK
                ), f"Expected {LOOKBACK} rows, got {len(rows)}"

                df = pd.DataFrame(rows)
                df["timestamps"] = pd.to_datetime(df["timestamps"], utc=True)

                x_df = df[["open", "high", "low", "close", "volume", "amount"]]
                x_timestamp = df["timestamps"]
                y_timestamp = pd.Series(
                    next_candle_timestamps(df["timestamps"].iloc[-1], PREDICT),
                    name="timestamps",
                )

                t0 = time.time()
                forecasts_df = self.predictor.predict(
                    df=x_df,
                    x_timestamp=x_timestamp,
                    y_timestamp=y_timestamp,
                    pred_len=PREDICT,
                    T=1.0,
                    top_p=0.9,
                    sample_count=10,
                )
                elapsed = time.time() - t0
                print(f"Inference: {elapsed:.2f}s")

                if store:
                    asof_ts = pd.Timestamp(asof_utc)
                    upsert_rows = [
                        (
                            "X:BTCUSD",
                            "15m",
                            asof_ts,
                            target_ts,
                            self.model_version,
                            row["open"],
                            row["high"],
                            row["low"],
                            row["close"],
                            row["volume"],
                            row["amount"],
                        )
                        for target_ts, row in forecasts_df.iterrows()
                    ]
                    cursor.executemany(UPSERT_FORECASTS, upsert_rows)
                    connection.commit()
                    print(f"Stored {len(upsert_rows)} forecast rows")

                return forecasts_df


if __name__ == "__main__":
    model_version = os.getenv("MODEL_VERSION", "XBTCUSD/15m/2025-09-01--2026-03-08")

    # First Kalshi KXBTC15M market of March 8th, 2026 ET
    # March 8th 00:00 ET = 05:00 UTC (still EST before DST at 2am)
    # asof = last candle we have data for, which is the candle OPENING at 04:45 UTC
    asof_utc = "2026-03-08 04:45:00+00"

    forecaster = Forecaster(model_version)
    result = forecaster.forecast(asof_utc)

    print(f"\nForecast from {asof_utc} (32 candles = 8 hours):")
    print(result[["open", "high", "low", "close"]].to_string())

    # Show the first candle prediction — this corresponds to the
    # 05:00-05:15 UTC candle = 00:00-00:15 ET March 8th (first Kalshi market)
    first = result.iloc[0]
    print(f"\nFirst Kalshi market (00:00-00:15 ET / 05:00-05:15 UTC):")
    print(f"  Predicted open:  ${first['open']:,.2f}")
    print(f"  Predicted close: ${first['close']:,.2f}")
    direction = "UP" if first["close"] > first["open"] else "DOWN"
    print(f"  Direction: {direction}")
