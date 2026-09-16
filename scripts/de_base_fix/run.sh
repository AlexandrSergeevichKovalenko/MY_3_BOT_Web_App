#!/usr/bin/env bash
# Правка немецко-русской базы. Запускать ВЛАДЕЛЬЦУ: агенту запись в прод закрыта.
#   ./run.sh              — сухой прогон, ничего не пишет
#   ./run.sh 20 --commit  — записать первые 20
set -euo pipefail

DATA="/private/tmp/claude-501/-Users-alexandr-Desktop-TELEGRAM-BOT-DEUTSCHESPRACHE/6a7dd7a6-343f-4b43-a5b6-c76786986144/scratchpad"
ROOT="/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE-english2"
LIMIT="${1:-20}"
FLAG="${2:-}"

if [ ! -d "$DATA" ]; then
  echo "Нет папки с данными прогона: $DATA" >&2
  exit 1
fi

URL=$(railway variables --service Postgres --json | python3 -c "import sys,json;print(json.load(sys.stdin)['DATABASE_PUBLIC_URL'])")

cd "$DATA"
DATA_DIR="$DATA" DB_URL="$URL" DATABASE_URL_DIRECT_RAILWAY="$URL" \
SKIP_STARTUP_SCHEMA_BOOTSTRAP=1 SKIP_BILLING_LEDGER_WRITES=1 \
python3 "$ROOT/scripts/de_base_fix/apply_fixes.py" --limit "$LIMIT" $FLAG

echo
if [ -f "$DATA/rollback.jsonl" ]; then
  echo "копий для отката записано: $(wc -l < "$DATA/rollback.jsonl" | tr -d ' ')"
else
  echo "копий для отката записано: 0"
fi
