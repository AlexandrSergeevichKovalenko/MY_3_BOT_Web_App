#!/bin/sh
# Прогнать тесты и, если они зелёные, запушить. Основной способ отправлять код.
#
#   ./scripts/push.sh                          # в текущую ветку (upstream)
#   ./scripts/push.sh bot3_webapp refactor/interface
#
# ЗАЧЕМ ОТДЕЛЬНАЯ КОМАНДА, а не просто `git push` с хуком. Хук pre-push устроен так, что
# git СНАЧАЛА открывает соединение с GitHub и забирает список веток, и только ПОТОМ
# отдаёт управление хуку. Наши тесты идут около 80 секунд, за это время соединение
# протухает, git переподключается — и переподключение висит 75 секунд и обрывается:
#
#   fatal: unable to access '…': Failed to connect to github.com port 443 after 75000 ms
#
# Это выглядело как капризная сеть и стоило многих потерянных попыток. Здесь порядок
# правильный: тесты прогоняются ДО того, как git полезет в сеть, а сам пуш идёт с
# --no-verify (прогон уже был) и укладывается в секунду.
#
# Прогон запоминается там же, где его ищет хук, поэтому обычный `git push` после этой
# команды тоже не будет гонять тесты второй раз.

set -e

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

# Тесты не должны трогать боевую базу: в окружении разработчика лежат боевые креденшелы.
export SKIP_STARTUP_SCHEMA_BOOTSTRAP=1

if ! python3 -c "import pytest" >/dev/null 2>&1; then
    echo "⛔️ pytest не установлен — прогнать тесты нечем."
    echo "   Если это осознанно:  git push --no-verify $*"
    exit 1
fi

echo "▶️  Тесты (около минуты)…"
if ! python3 -m pytest backend/tests -q; then
    echo ""
    echo "⛔️ Пуш не начат: тесты красные."
    echo "   Запушить всё равно:  git push --no-verify $*"
    exit 1
fi

# Запомнить прогон ровно в том же виде, в каком его проверяет хук, — чтобы обычный
# `git push` следом не гонял тесты заново. Отпечаток берём по тому коду, который эти
# тесты проверяют: дерево backend/ в коммите + незакоммиченные правки в backend/ и
# scripts/. Формула обязана совпадать с code_fingerprint в scripts/git-hooks/pre-push.
VERIFIED_FILE=$(git rev-parse --git-path pre-push-verified 2>/dev/null || true)
if [ -n "$VERIFIED_FILE" ]; then
    BACKEND_TREE=$(git rev-parse HEAD:backend 2>/dev/null || echo "no-tree")
    DIRTY_SHA=$(git diff HEAD -- backend scripts 2>/dev/null | git hash-object --stdin 2>/dev/null || echo "no-diff")
    echo "$BACKEND_TREE:$DIRTY_SHA" >> "$VERIFIED_FILE" 2>/dev/null || true
    if tail -n 20 "$VERIFIED_FILE" > "$VERIFIED_FILE.tmp" 2>/dev/null; then
        mv "$VERIFIED_FILE.tmp" "$VERIFIED_FILE" 2>/dev/null || rm -f "$VERIFIED_FILE.tmp"
    fi
fi

echo ""
echo "✅ Тесты зелёные — пушу."
# --no-verify здесь не «в обход проверки», а «проверка уже прошла, строкой выше».
git push --no-verify "$@"
