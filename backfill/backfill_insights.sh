#!/bin/bash
# Backfill insights in N-day batches to respect Meta API rate limits.
# Usage: ./backfill_insights.sh [START_DATE] [END_DATE]
#   Defaults: START=2026-01-01, END=yesterday
#
# Env overrides:
#   PYTHON         python interpreter (default: <repo>/venv/bin/python, else python3)
#   BATCH_DAYS     days per batch (default 20)
#   SLEEP_BETWEEN  seconds to sleep between batches (default 60)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$REPO_DIR/venv/bin/python}"
[ -x "$PYTHON" ] || PYTHON="python3"
SCRIPT="$REPO_DIR/insights.py"
LOG_DIR="${LOG_DIR:-$REPO_DIR/logs}"
BATCH_DAYS="${BATCH_DAYS:-20}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-60}"

START="${1:-2026-01-01}"
END="${2:-$(date -d 'yesterday' +%Y-%m-%d)}"

mkdir -p "$LOG_DIR"

echo "================================================"
echo " Insights backfill: $START -> $END (${BATCH_DAYS}-day batches)"
echo "================================================"

current="$START"
batch=1

while [[ "$current" < "$END" || "$current" == "$END" ]]; do
    batch_end=$(date -d "$current + $((BATCH_DAYS - 1)) days" +%Y-%m-%d)
    if [[ "$batch_end" > "$END" ]]; then
        batch_end="$END"
    fi

    LOG="$LOG_DIR/backfill_insights_batch${batch}_${current}_${batch_end}.log"
    echo ""
    echo "[Batch $batch] $current -> $batch_end"
    echo "  Log: $LOG"

    INSIGHTS_START_DATE="$current" INSIGHTS_END_DATE="$batch_end" \
        "$PYTHON" "$SCRIPT" 2>&1 | tee "$LOG"

    EXIT=${PIPESTATUS[0]}
    if [ "$EXIT" -ne 0 ]; then
        echo "[ERROR] Batch $batch failed with exit code $EXIT. Stopping."
        exit "$EXIT"
    fi

    echo "[Batch $batch] done"

    current=$(date -d "$batch_end + 1 day" +%Y-%m-%d)
    batch=$((batch + 1))

    if [[ "$current" < "$END" || "$current" == "$END" ]]; then
        echo "  Sleeping ${SLEEP_BETWEEN}s before next batch..."
        sleep "$SLEEP_BETWEEN"
    fi
done

echo ""
echo "================================================"
echo " Backfill complete -- $((batch - 1)) batches processed"
echo "================================================"
