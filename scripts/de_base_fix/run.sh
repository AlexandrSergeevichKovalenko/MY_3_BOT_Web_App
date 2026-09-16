#!/usr/bin/env bash
# Правка немецко-русской базы. Запускать ВЛАДЕЛЬЦУ (агенту запись в прод закрыта).
#   ./run.sh              — сухой прогон, ничего не пишет
#   ./run.sh 20 --commit  — записать первые 20
set -euo pipefail
ДАННЫЕ="/private/tmp/claude-501/-Users-alexandr-Desktop-TELEGRAM-BOT-DEUTSCHESPRACHE/6a7dd7a6-343f-4b43-a5b6-c76786986144/scratchpad"
КОРЕНЬ="/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE-english2"
ЛИМИТ="${1:-20}"; ФЛАГ="${2:-}"
URL=$(railway variables --service Postgres --json | python3 -c "import sys,json;print(json.load(sys.stdin)['DATABASE_PUBLIC_URL'])")
cd "$ДАННЫЕ"
DB_URL="$URL" DATABASE_URL_DIRECT_RAILWAY="$URL" \
SKIP_STARTUP_SCHEMA_BOOTSTRAP=1 SKIP_BILLING_LEDGER_WRITES=1 \
python3 "$КОРЕНЬ/scripts/de_base_fix/apply_fixes.py" --limit "$ЛИМИТ" $ФЛАГ
echo
echo "копий для отката записано: $(wc -l < "$ДАННЫЕ/rollback.jsonl" 2>/dev/null || echo 0)"
