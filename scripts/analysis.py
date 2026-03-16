import json
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from db import conn

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
MODEL = "XBTCUSD/15m/2025-09-01--2026-03-08"

ALL_FORECASTS = """
SELECT target_timestamp, asof_timestamp,
       open::float, high::float, low::float, close::float
FROM forecasts
WHERE symbol = 'X:BTCUSD' AND interval = '15m'
  AND model_version = %s
  AND target_timestamp >= %s AND target_timestamp <= %s
ORDER BY target_timestamp, asof_timestamp
"""

ACTUAL_CANDLES = """
SELECT timestamp, open::float, high::float, low::float, close::float
FROM candles
WHERE symbol = 'X:BTCUSD' AND interval = '15m'
  AND timestamp >= %s AND timestamp <= %s
ORDER BY timestamp
"""


def fetch_kalshi(min_ts, max_ts):
    url = (
        f"{KALSHI_BASE}/markets?series_ticker=KXBTC15M&status=settled"
        f"&limit=200&min_close_ts={min_ts}&max_close_ts={max_ts}"
    )
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read())
    markets = {}
    for m in data["markets"]:
        if not m.get("floor_strike") or not m.get("expiration_value"):
            continue
        ot = datetime.fromisoformat(m["open_time"].replace("Z", "+00:00"))
        markets[ot] = {
            "ticker": m["ticker"],
            "strike": float(m["floor_strike"]),
            "expiry_val": float(m["expiration_value"]),
            "result": m["result"],
        }
    return markets


def main():
    # Usage: python -m scripts.analysis 2026-03-08 -5
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-03-08"
    utc_offset = int(sys.argv[2]) if len(sys.argv) > 2 else -5

    d = datetime.fromisoformat(date_str)
    # First candle open: midnight ET → UTC
    first_utc = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) - timedelta(
        hours=utc_offset
    )
    # Last candle open: 23:45 ET → UTC
    last_utc = first_utc + timedelta(hours=23, minutes=45)
    # Kalshi close timestamps: first_utc + 15m to last_utc + 15m
    min_close_ts = int((first_utc + timedelta(minutes=15)).timestamp())
    max_close_ts = int((last_utc + timedelta(minutes=16)).timestamp())

    first_str = first_utc.strftime("%Y-%m-%d %H:%M+00")
    last_str = last_utc.strftime("%Y-%m-%d %H:%M+00")

    print(f"Analyzing {date_str} ET (UTC offset {utc_offset})")
    print(f"  UTC range: {first_str} to {last_str}")

    kalshi = fetch_kalshi(min_close_ts, max_close_ts)
    print(f"  Kalshi markets: {len(kalshi)}")

    with conn() as connection:
        with connection.cursor() as cur:
            cur.execute(ALL_FORECASTS, (MODEL, first_str, last_str))
            fc_rows = cur.fetchall()
            cur.execute(ACTUAL_CANDLES, (first_str, last_str))
            candle_rows = cur.fetchall()

    fc_df = pd.DataFrame(fc_rows)
    fc_df.rename(
        columns={"target_timestamp": "target", "asof_timestamp": "asof"}, inplace=True
    )
    fc_df = fc_df[["target", "asof", "open", "high", "low", "close"]].copy()
    for c in ["open", "high", "low", "close"]:
        fc_df[c] = fc_df[c].astype(float)
    candles = {
        r["timestamp"]: {k: float(r[k]) for k in ["open", "high", "low", "close"]}
        for r in candle_rows
    }

    # Add step number (1 = most recent forecast, 32 = oldest)
    fc_df["step"] = fc_df.groupby("target")["asof"].rank(ascending=False).astype(int)

    # Build per-target dataset
    targets = sorted(fc_df["target"].unique())
    rows = []
    for t in targets:
        if t not in kalshi:
            continue
        k = kalshi[t]
        tfc = fc_df[fc_df["target"] == t].sort_values("asof")
        actual = candles.get(t)

        for _, f in tfc.iterrows():
            rows.append(
                {
                    "target": t,
                    "step": f["step"],
                    "pred_open": f["open"],
                    "pred_high": f["high"],
                    "pred_low": f["low"],
                    "pred_close": f["close"],
                    "strike": k["strike"],
                    "expiry_val": k["expiry_val"],
                    "actual_result": k["result"],
                    "actual_close": actual["close"] if actual else None,
                    "actual_open": actual["open"] if actual else None,
                }
            )

    df = pd.DataFrame(rows)
    df["pred_yes"] = df["pred_close"] >= df["strike"]
    df["actual_yes"] = df["actual_result"] == "yes"
    df["hit"] = df["pred_yes"] == df["actual_yes"]
    df["diff"] = df["pred_close"] - df["strike"]
    df["abs_diff"] = df["diff"].abs()

    n_markets = df["target"].nunique()
    print(f"Analyzing {n_markets} markets, {len(df)} forecast rows\n")

    # ── 1. Per-step hit rate ──
    print("=" * 60)
    print("PER-STEP HIT RATE (1=most recent, 32=oldest)")
    print("=" * 60)
    step_hits = df.groupby("step")["hit"].agg(["sum", "count", "mean"])
    step_hits.columns = ["hits", "total", "rate"]
    best_step = step_hits["rate"].idxmax()
    for step in range(1, 33):
        r = step_hits.loc[step]
        marker = " <<<" if step == best_step else ""
        bar = "#" * int(r["rate"] * 40)
        print(
            f"  Step {step:2d}: {r['rate']:.1%}  ({int(r['hits'])}/{int(r['total'])})  {bar}{marker}"
        )

    # ── 2. Aggregation methods ──
    print(f"\n{'=' * 60}")
    print("AGGREGATION METHODS")
    print("=" * 60)

    def eval_method(name, pred_series):
        merged = pred_series.reset_index()
        merged.columns = ["target", "pred_close"]
        merged = merged.merge(
            df[["target", "strike", "actual_result"]].drop_duplicates(),
            on="target",
        )
        merged["hit"] = (merged["pred_close"] >= merged["strike"]) == (
            merged["actual_result"] == "yes"
        )
        rate = merged["hit"].mean()
        print(f"  {name:30s}: {rate:.1%}  ({merged['hit'].sum()}/{len(merged)})")
        return rate

    # Simple mean
    eval_method("Simple mean (all 32)", df.groupby("target")["pred_close"].mean())
    eval_method("Median (all 32)", df.groupby("target")["pred_close"].median())
    eval_method(
        "Step 1 only (most recent)",
        df[df["step"] == 1].set_index("target")["pred_close"],
    )

    for s in [2, 3, 4, 5, 6, 8, 10, 16]:
        eval_method(
            f"Step {s} only", df[df["step"] == s].set_index("target")["pred_close"]
        )

    # Best N steps
    for n in [3, 5, 8]:
        sub = df[df["step"].between(best_step - n // 2, best_step + n // 2)]
        eval_method(
            f"Mean of steps {best_step - n//2}-{best_step + n//2}",
            sub.groupby("target")["pred_close"].mean(),
        )

    # EMA sweep
    print(f"\n{'=' * 60}")
    print("EMA ALPHA SWEEP")
    print("=" * 60)
    for alpha in np.arange(0.05, 0.55, 0.05):

        def ema_close(group, a=alpha):
            g = group.sort_values("step", ascending=False)
            n = len(g)
            w = np.array([a * (1 - a) ** (n - 1 - i) for i in range(n)])
            w /= w.sum()
            return (g["pred_close"].values * w).sum()

        preds = df.groupby("target").apply(ema_close)
        eval_method(f"EMA alpha={alpha:.2f}", preds)

    # ── 3. Confidence filtering on best step ──
    print(f"\n{'=' * 60}")
    print(f"CONFIDENCE FILTERING (step={best_step})")
    print("=" * 60)
    best = df[df["step"] == best_step].copy()
    for thresh in [0, 50, 100, 150, 200, 250, 300, 400, 500]:
        sub = best[best["abs_diff"] >= thresh]
        if len(sub) < 3:
            continue
        rate = sub["hit"].mean()
        print(
            f"  |diff| >= ${thresh:4d}:  {rate:.1%}  ({sub['hit'].sum()}/{len(sub)} markets)"
        )

    # ── 4. Direction signals ──
    print(f"\n{'=' * 60}")
    print("DIRECTION SIGNALS (step={})".format(best_step))
    print("=" * 60)
    best = df[df["step"] == best_step].copy()
    best["pred_direction"] = best["pred_close"] > best["pred_open"]
    best["actual_direction"] = best["expiry_val"] > best["strike"]
    best["dir_hit"] = best["pred_direction"] == best["actual_direction"]
    print(f"  pred_close vs pred_open direction: {best['dir_hit'].mean():.1%}")

    # pred_close vs strike (our main method)
    print(f"  pred_close vs strike:              {best['hit'].mean():.1%}")

    # High/low signals
    best["high_above_strike"] = best["pred_high"] > best["strike"]
    best["low_below_strike"] = best["pred_low"] < best["strike"]
    best["range"] = best["pred_high"] - best["pred_low"]

    # When predicted range straddles strike vs doesn't
    straddles = best[
        (best["pred_low"] < best["strike"]) & (best["pred_high"] > best["strike"])
    ]
    no_straddle = best[
        ~((best["pred_low"] < best["strike"]) & (best["pred_high"] > best["strike"]))
    ]
    print(
        f"\n  Strike INSIDE pred range:  {straddles['hit'].mean():.1%}  ({len(straddles)} markets)"
    )
    print(
        f"  Strike OUTSIDE pred range: {no_straddle['hit'].mean():.1%}  ({len(no_straddle)} markets)"
    )

    # Range quartiles
    best["range_q"] = pd.qcut(
        best["range"], 4, labels=["Q1 narrow", "Q2", "Q3", "Q4 wide"]
    )
    for q in ["Q1 narrow", "Q2", "Q3", "Q4 wide"]:
        sub = best[best["range_q"] == q]
        print(f"  Range {q}: {sub['hit'].mean():.1%}  ({len(sub)})")

    # ── 5. Consensus voting ──
    print(f"\n{'=' * 60}")
    print("CONSENSUS VOTING (all 32 forecasts per target)")
    print("=" * 60)
    consensus = df.groupby("target").agg(
        yes_votes=("pred_yes", "sum"),
        total=("pred_yes", "count"),
        actual=("actual_yes", "first"),
    )
    consensus["pct_yes"] = consensus["yes_votes"] / consensus["total"]
    consensus["pred"] = consensus["pct_yes"] >= 0.5
    consensus["hit"] = consensus["pred"] == consensus["actual"]

    print(f"  Simple majority (>50%): {consensus['hit'].mean():.1%}")

    for thresh in [0.55, 0.6, 0.65, 0.7, 0.75, 0.8]:
        high = consensus[
            (consensus["pct_yes"] >= thresh) | (consensus["pct_yes"] <= 1 - thresh)
        ]
        if len(high) < 3:
            continue
        high_hit = ((high["pct_yes"] >= thresh) == high["actual"]) | (
            (high["pct_yes"] <= 1 - thresh) == ~high["actual"]
        )
        print(
            f"  Supermajority >={thresh:.0%}: {high_hit.mean():.1%}  ({len(high)} markets)"
        )

    # ── 6. Combining step 4 + consensus ──
    print(f"\n{'=' * 60}")
    print("COMBINED STRATEGIES")
    print("=" * 60)
    best = df[df["step"] == best_step].copy().set_index("target")
    consensus_pct = consensus["pct_yes"]

    for conf_thresh in [100, 200, 300]:
        for cons_thresh in [0.5, 0.6, 0.7]:
            mask = (best["abs_diff"] >= conf_thresh) & (
                (consensus_pct.reindex(best.index) >= cons_thresh)
                | (consensus_pct.reindex(best.index) <= 1 - cons_thresh)
            )
            # Also check that step-4 and consensus agree
            sub = best[mask]
            step_pred = sub["pred_yes"]
            cons_pred = consensus_pct.reindex(sub.index) >= 0.5
            agree = sub[step_pred == cons_pred]
            if len(agree) < 3:
                continue
            rate = agree["hit"].mean()
            print(
                f"  Step {best_step} |diff|>=${conf_thresh} + consensus>{cons_thresh:.0%} + agree: "
                f"{rate:.1%}  ({agree['hit'].sum()}/{len(agree)} markets)"
            )

    # ── 7. Time-of-day ──
    print(f"\n{'=' * 60}")
    print("TIME OF DAY (ET hours, step={})".format(best_step))
    print("=" * 60)
    best = df[df["step"] == best_step].copy()

    # Convert target UTC to ET (March 8 before 07:00 UTC is EST=UTC-5, after is EDT=UTC-4)
    def utc_to_et_hour(ts):
        utc_h = ts.hour
        # DST transition: March 8 at 07:00 UTC = 2:00 AM EST → 3:00 AM EDT
        if ts.date() == datetime(2026, 3, 8).date() and utc_h < 7:
            return (utc_h - 5) % 24  # EST
        return (utc_h - 4) % 24  # EDT

    best["et_hour"] = best["target"].apply(utc_to_et_hour)
    hourly = best.groupby("et_hour")["hit"].agg(["sum", "count", "mean"])
    hourly.columns = ["hits", "total", "rate"]
    for h in range(24):
        if h in hourly.index:
            r = hourly.loc[h]
            bar = "#" * int(r["rate"] * 20)
            print(
                f"  {h:2d}:00 ET: {r['rate']:.1%}  ({int(r['hits'])}/{int(r['total'])})  {bar}"
            )

    # ── 8. Prediction error vs actuals ──
    print(f"\n{'=' * 60}")
    print("PREDICTION ERROR vs ACTUAL CANDLES (step={})".format(best_step))
    print("=" * 60)
    best = df[df["step"] == best_step].copy()
    best = best.dropna(subset=["actual_close", "actual_open"])
    for col in ["close", "open"]:
        err = best[f"pred_{col}"] - best[f"actual_{col}"]
        print(
            f"  {col:5s}  MAE=${err.abs().mean():,.0f}  RMSE=${np.sqrt((err**2).mean()):,.0f}  "
            f"bias=${err.mean():+,.0f}  median_err=${err.median():+,.0f}"
        )


if __name__ == "__main__":
    main()
