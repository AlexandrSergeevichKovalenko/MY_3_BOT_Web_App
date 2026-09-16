#!/usr/bin/env bash
# Локальный стенд: точная копия прод-базы для проверки правок ДО прода.
#
# Зачем. 16.09.2026 три попытки записи в прод провалились на ошибках, которые стенд
# поймал бы за секунду: имя колонки, порядок FK, синтаксис shell. Владелец запускал
# руками и ловил их за меня. Стенд снимает это целиком.
#
# ⚠ Версия PostgreSQL должна СОВПАДАТЬ с продом (18.6). Наш нагрузочный стенд на 16 —
# он для замера скорости, а не для проверки схемы, и для этой задачи не годится:
# pg_dump 17 вообще отказывается снимать с сервера 18.
set -euo pipefail

NAME=en_stand_pg
PORT=55433
URL_LOCAL="postgresql://stand:stand@127.0.0.1:${PORT}/stand"

echo "→ поднимаю PostgreSQL 18 (данные в памяти, исчезают вместе с контейнером)"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" \
  -e POSTGRES_USER=stand -e POSTGRES_PASSWORD=stand -e POSTGRES_DB=stand \
  -p "127.0.0.1:${PORT}:5432" \
  --tmpfs /var/lib/postgresql:rw,size=2g postgres:18 >/dev/null
for _ in $(seq 1 30); do docker exec "$NAME" pg_isready -U stand >/dev/null 2>&1 && break; sleep 2; done

echo "→ беру адрес прода"
PROD=$(railway variables --service Postgres --json | python3 -c "import sys,json;print(json.load(sys.stdin)['DATABASE_PUBLIC_URL'])")

echo "→ снимаю СХЕМУ (только DDL) клиентом 18 из контейнера"
docker run --rm -e U="$PROD" postgres:18 sh -c 'pg_dump --schema-only --no-owner --no-privileges "$U"' > /tmp/stand_schema.sql
docker cp /tmp/stand_schema.sql "$NAME":/tmp/schema.sql >/dev/null
docker exec "$NAME" psql -U stand -d stand -q -f /tmp/schema.sql

# ⚠ ПОРЯДОК ВАЖЕН: связи ссылаются на значения внешним ключом. Если лить связи
# раньше значений, ВСЕ 71 080 связей отваливаются молча — проверено 16.09.2026.
echo "→ заливаю данные (значения ДО связей)"
for t in bt_3_lex_units bt_3_lex_senses bt_3_lex_surfaces bt_3_lex_links \
         bt_3_webapp_dictionary_queries bt_3_dictionary_entries \
         bt_3_user_word_overrides bt_3_phrase_review bt_3_user_word_review; do
  docker run --rm -e U="$PROD" -e T="$t" postgres:18 \
    sh -c 'pg_dump --data-only --no-owner --table="$T" "$U"' > "/tmp/stand_${t}.sql"
  docker cp "/tmp/stand_${t}.sql" "$NAME":/tmp/d.sql >/dev/null
  docker exec "$NAME" psql -U stand -d stand -q -f /tmp/d.sql
  echo "   ✔ $t"
done

echo
echo "Стенд готов: $URL_LOCAL"
docker exec "$NAME" psql -U stand -d stand -c "
SELECT 'lex_units' AS t, count(*) FROM bt_3_lex_units
UNION ALL SELECT 'lex_links', count(*) FROM bt_3_lex_links
UNION ALL SELECT 'личные карточки', count(*) FROM bt_3_webapp_dictionary_queries;"
