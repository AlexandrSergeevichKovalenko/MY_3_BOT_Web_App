#!/usr/bin/env bash
# Запись английской стороны в базу. Запускать ВЛАДЕЛЬЦУ: агенту запись в прод закрыта.
#
#   ./en_bridge_run.sh                  — сухой прогон, ничего не пишет
#   ./en_bridge_run.sh стенд --commit   — записать в ЛОКАЛЬНЫЙ стенд
#   ./en_bridge_run.sh прод  --commit   — записать в ПРОД
#
# Операция только ДОБАВЛЯЕТ: английские единицы и связи немецкое→английское.
# Ничего не удаляет, ничего не понижает, немецкой и русской стороны не касается.
# Проверено целиком на стенде 16.09.2026: все счётчики немецкой стороны до/после совпали.
set -euo pipefail

DATA="/private/tmp/claude-501/-Users-alexandr-Desktop-TELEGRAM-BOT-DEUTSCHESPRACHE/6a7dd7a6-343f-4b43-a5b6-c76786986144/scratchpad"
ROOT="/Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE-english2"
WHERE="${1:-прод}"
FLAG="${2:-}"

if [ ! -f "$DATA/bridge_out.jsonl" ]; then
  echo "Нет посчитанной английской стороны: $DATA/bridge_out.jsonl" >&2
  exit 1
fi

case "$WHERE" in
  стенд|stand)
    # ⚠ У стенда нет SSL, а код требует защищённое соединение. Без sslmode=disable
    # дверь единиц отвергает ВСЁ и тратит 6 минут на 200 записей (поймано 16.09.2026).
    URL="postgresql://stand:stand@127.0.0.1:55433/stand?sslmode=disable" ;;
  прод|prod)
    URL=$(railway variables --service Postgres --json | python3 -c "import sys,json;print(json.load(sys.stdin)['DATABASE_PUBLIC_URL'])") ;;
  *) echo "Куда писать: стенд или прод" >&2; exit 1 ;;
esac

echo "адрес: $(echo "$URL" | sed -E 's#://[^@]*@#://…@#')"
cd "$DATA"
JOURNAL="bridge_write_${WHERE}.jsonl" DATA_DIR="$DATA" DB_URL="$URL" DATABASE_URL_DIRECT_RAILWAY="$URL" \
SKIP_STARTUP_SCHEMA_BOOTSTRAP=1 SKIP_BILLING_LEDGER_WRITES=1 \
python3 "$ROOT/scripts/en_bridge_write.py" $FLAG

echo
J="$DATA/bridge_write_${WHERE}.jsonl"
if [ -f "$J" ]; then
  echo "записей в журнале ($WHERE): $(wc -l < "$J" | tr -d ' ')"
fi
