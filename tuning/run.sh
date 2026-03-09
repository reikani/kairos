#!/bin/bash
set -euo pipefail

# Required environment variables:
# - BASTION_SSH_KEY
# - MASSIVE_API_KEY
# - START         (training start date, e.g. 2025-09-01)
# - END           (training end date, e.g. 2026-03-08 — data up to but not including this date)

export SYMBOL="${SYMBOL:-X:BTCUSD}"
export INTERVAL="${INTERVAL:-15m}"
export SERVER="${SERVER:-localhost}"
export LOOKBACK_WINDOW="${LOOKBACK_WINDOW:-160}"
export PREDICT_WINDOW="${PREDICT_WINDOW:-32}"

# Sanitize symbol for filesystem paths (X:BTCUSD -> BTCUSD)
SYMBOL_SAFE="${SYMBOL//:/}"

# System
echo "$BASTION_SSH_KEY" > /tmp/bastion
chmod 600 /tmp/bastion
ssh -f -N \
  -i /tmp/bastion \
  -o ExitOnForwardFailure=yes \
  -o IdentitiesOnly=yes \
  -o ServerAliveInterval=30 \
  -o StrictHostKeyChecking=no \
  -L 5432:mini:5432 \
  -L 9000:mini:9000 \
  bastion@bastion.hazardlabs.dev
mc alias set mini http://$SERVER:9000 minioadmin minioadmin

# Database — ensure kairos DB and tables exist
PGPASSWORD=postgres createdb -h localhost -U postgres kairos 2>/dev/null || true
PGPASSWORD=postgres psql -h localhost -U postgres -d kairos -f db/01_candles.sql

# Data — ingest from Massive into kairos DB, then export CSV
python -m scripts.export
mc cp data.csv mini/models/$SYMBOL_SAFE/$INTERVAL/$START--$END/

# Config — use sanitized symbol for filesystem paths
SYMBOL_ORIG="$SYMBOL"
export SYMBOL="$SYMBOL_SAFE"
envsubst < config.yaml.tmpl > config.yaml
export SYMBOL="$SYMBOL_ORIG"
mc cp config.yaml mini/models/$SYMBOL_SAFE/$INTERVAL/$START--$END/

# Tuning
export GPU_COUNT=$(nvidia-smi -L | wc -l)
export DIST_BACKEND=nccl OMP_NUM_THREADS=$GPU_COUNT
torchrun --standalone --nproc_per_node=$GPU_COUNT train_sequential.py --skip-basemodel
sleep 3
torchrun --standalone --nproc_per_node=$GPU_COUNT train_sequential.py --skip-tokenizer

# Store
mc cp -r ./tuned/ mini/models/

# Remove
runpodctl remove pod $RUNPOD_POD_ID
