#!/bin/sh
# Фронтовые тесты. Одна команда для хука pre-push и для ./scripts/push.sh.
#
# ЗАЧЕМ ПОЯВИЛСЯ (24.08.2026). В frontend/ лежало 18 файлов тестов — на node:test,
# без браузера и без сборки, — и их не запускал НИКТО: ни хук, ни деплой. То есть
# сторож стоял, но его не будили: правка могла спокойно уехать в прод мимо своего
# же теста. Нашлось это, когда к правке автоопределения языков дописывался тест и
# выяснилось, что гонять его нечем, кроме как руками.
#
# ПОЧЕМУ БЕЗ ПАМЯТИ О ЗЕЛЁНОМ ПРОГОНЕ, в отличие от backend/tests. Тот набор идёт
# около минуты, и ради него в хуке живёт отпечаток кода. Здесь весь прогон — 0,4
# секунды: кешировать нечего, а лишний механизм — это лишний способ соврать
# «зелено» там, где не проверяли. Поэтому просто гоняем всегда.
#
# Прогнать руками:  ./scripts/test-frontend.sh

REPO_ROOT=$(git rev-parse --show-toplevel) || exit 0
cd "$REPO_ROOT" || exit 0

# Тесты лежат в двух местах: собственный каталог и один файл рядом с исходником.
# Перечислены явно, чтобы новый каталог не подхватился молча и без ведома.
FRONT_TESTS="frontend/tests/*.test.mjs frontend/src/utils/*.test.mjs"

# shellcheck disable=SC2086
if ! ls $FRONT_TESTS >/dev/null 2>&1; then
    echo "⚠️  Фронтовых тестов не нашлось по путям: $FRONT_TESTS"
    echo "   Прогон пропущен. Если тесты переехали — поправьте scripts/test-frontend.sh."
    exit 0
fi

if ! command -v node >/dev/null 2>&1; then
    echo "⚠️  node не найден — фронтовые тесты прогнать нечем, пропускаю."
    exit 0
fi

# shellcheck disable=SC2086
OUTPUT=$(node --test $FRONT_TESTS 2>&1)
STATUS=$?

if [ $STATUS -eq 0 ]; then
    echo "$OUTPUT" | grep -E "^ℹ (tests|pass|fail) " | tr '\n' ' '
    echo ""
    echo "✅ Фронтовые тесты зелёные."
    exit 0
fi

echo ""
echo "$OUTPUT" | grep -E "^(not ok|✖)" | head -n 20
echo "$OUTPUT" | grep -E "^ℹ (tests|pass|fail) "
echo ""
echo "⛔️ Фронтовые тесты красные."
echo "   Посмотреть подробно:  node --test $FRONT_TESTS"
exit 1
