# Kairos — Kronos + Kalshi BTC Trading

BTC 15-minute forecasting system using the Kronos foundation model, targeting Kalshi's KXBTC15M binary markets. Fine-tunes Kronos on 15m BTC candle data to forecast whether BTC goes up or down in each 15-minute window.

## Architecture

### Data Pipeline

1. **Ingest** (`scripts/ingest.py`): Fetches 15m BTC candles from Massive API (`X:BTCUSD`) into PostgreSQL
2. **Export** (`scripts/export.py`): Exports candles to CSV for training
3. **Database** (`db/`): PostgreSQL `kairos` database — no market sessions needed (BTC trades 24/7 UTC)

### Model

Same Kronos architecture as hobbes-spy:
- **KronosTokenizer**: Encoder/decoder transformer with BSQuantizer
- **Kronos**: 12-layer transformer predictor
- **KronosPredictor**: Inference wrapper

Pre-trained from HuggingFace: `NeoQuasar/Kronos-Tokenizer-base`, `NeoQuasar/Kronos-base`

### Training

Two-phase sequential fine-tuning via `tuning/train_sequential.py`:
1. Tokenizer phase (30 epochs)
2. Basemodel phase (20 epochs)

### Key Parameters

| Parameter | Value |
|-----------|-------|
| Symbol | `X:BTCUSD` |
| Interval | `15m` |
| Lookback | 160 candles (40 hours) |
| Forecast | 32 candles (8 hours) |

## Commands

```bash
# Export data
MASSIVE_API_KEY=xxx SYMBOL=X:BTCUSD INTERVAL=15m START=2025-09-01 END=2026-03-07 python -m scripts.export

# Training (on Runpod)
cd tuning
torchrun --standalone --nproc_per_node=$GPU_COUNT train_sequential.py
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SYMBOL` | Ticker | `X:BTCUSD` |
| `INTERVAL` | Candle interval | `15m` |
| `START` | Training start date | `2025-09-01` |
| `END` | Training end date | `2026-03-07` |
| `LOOKBACK_WINDOW` | Context window | `160` |
| `PREDICT_WINDOW` | Forecast horizon | `32` |
| `MASSIVE_API_KEY` | Massive API key | - |
| `SERVER` | DB/MinIO host | `mini` |
| `BASTION_SSH_KEY` | SSH key for tunnel | - |

## Infrastructure

- Training on RunPod (multi-GPU, NCCL)
- SSH tunnel via bastion for DB access
- MinIO for model storage
- PostgreSQL `kairos` database (same instance as `hobbes`)
