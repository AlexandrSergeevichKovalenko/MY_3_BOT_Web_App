#!/usr/bin/env bash
#
# agent-worktree.sh — дать агенту ИЗОЛИРОВАННУЮ рабочую папку на общем репозитории.
#
# Каждый агент работает в своём каталоге и на своей ветке, поэтому они физически
# не видят файлы друг друга и не могут затереть чужой незакоммиченный WIP или
# сбить HEAD ребейзом. .git общий (место экономится), node_modules симлинкуется
# из основного frontend (не нужно гонять `npm ci` заново).
#
# Использование:
#   ./agent-worktree.sh <имя-агента> [база]     # создать / открыть worktree
#   ./agent-worktree.sh --list                  # показать все worktree
#   ./agent-worktree.sh --remove <имя-агента>   # удалить worktree агента
#
# Примеры:
#   ./agent-worktree.sh alice                   # -> ../<repo>-alice, ветка agent/alice
#   ./agent-worktree.sh bob main                # форк от main
#
set -euo pipefail

REMOTE="bot3_webapp"                     # ваш пуш-remote (см. `git remote -v`)
DEFAULT_BASE="${REMOTE}/refactor/interface"

MAIN_ROOT="$(git rev-parse --show-toplevel)"
REPO_NAME="$(basename "$MAIN_ROOT")"

cmd="${1:-}"

if [[ "$cmd" == "--list" || "$cmd" == "-l" ]]; then
  git -C "$MAIN_ROOT" worktree list
  exit 0
fi

if [[ "$cmd" == "--remove" || "$cmd" == "-r" ]]; then
  name="${2:?Укажи имя агента: ./agent-worktree.sh --remove <имя>}"
  wt="$(dirname "$MAIN_ROOT")/${REPO_NAME}-${name}"
  rm -f "$wt/frontend/node_modules"            # снять симлинк, чтобы не ушли в удаление
  git -C "$MAIN_ROOT" worktree remove --force "$wt"
  git -C "$MAIN_ROOT" worktree prune
  echo "✓ worktree агента '$name' удалён ($wt). Ветку agent/$name при желании удали вручную."
  exit 0
fi

NAME="${cmd:?Использование: ./agent-worktree.sh <имя-агента> [база]}"
BASE="${2:-$DEFAULT_BASE}"
WT_DIR="$(dirname "$MAIN_ROOT")/${REPO_NAME}-${NAME}"
BRANCH="agent/${NAME}"

echo "→ Обновляю базу ($REMOTE)…"
git -C "$MAIN_ROOT" fetch --prune "$REMOTE"

if git -C "$MAIN_ROOT" show-ref --verify --quiet "refs/heads/${BRANCH}"; then
  echo "→ Ветка $BRANCH уже есть — открываю worktree на ней."
  git -C "$MAIN_ROOT" worktree add "$WT_DIR" "$BRANCH"
else
  echo "→ Создаю ветку $BRANCH от $BASE."
  git -C "$MAIN_ROOT" worktree add "$WT_DIR" -b "$BRANCH" "$BASE"
fi

# node_modules: симлинк на основной frontend (быстро; общий кеш зависимостей).
# Если агент меняет зависимости — пусть снимет симлинк и сделает свой `npm ci`.
if [[ -d "$MAIN_ROOT/frontend/node_modules" ]]; then
  ln -sfn "$MAIN_ROOT/frontend/node_modules" "$WT_DIR/frontend/node_modules"
  echo "✓ frontend/node_modules симлинкнут из основного репо"
else
  echo "! В основном репо нет frontend/node_modules — сделай 'cd $WT_DIR/frontend && npm ci' один раз"
fi

cat <<EOF

✓ Готово. Изолированная папка для агента '$NAME':
    каталог: $WT_DIR
    ветка:   $BRANCH  (от $BASE)

Дальше:
  • агент работает ТОЛЬКО в $WT_DIR
  • коммитит маленько и часто
  • когда готово:
      cd "$WT_DIR"
      git push -u $REMOTE $BRANCH
      # затем PR из $BRANCH в refactor/interface
  • удалить, когда закончил:
      ./agent-worktree.sh --remove $NAME
EOF
