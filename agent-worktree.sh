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
#   ./agent-worktree.sh --merge <имя-агента>    # влить ветку агента в refactor/interface + запушить + убрать worktree
#   ./agent-worktree.sh --list                  # показать все worktree
#   ./agent-worktree.sh --remove <имя-агента>   # удалить worktree агента (без слияния)
#
# Примеры:
#   ./agent-worktree.sh alice                   # -> ../<repo>-alice, ветка agent/alice
#   ./agent-worktree.sh bob main                # форк от main
#   ./agent-worktree.sh --merge alice           # принять работу alice в refactor/interface
#
set -euo pipefail

REMOTE="${WT_REMOTE:-bot3_webapp}"       # ваш пуш-remote (см. `git remote -v`)
TARGET="${WT_TARGET:-refactor/interface}"  # основная ветка, куда вливаем
DEFAULT_BASE="${REMOTE}/${TARGET}"

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

if [[ "$cmd" == "--merge" || "$cmd" == "-m" ]]; then
  name="${2:?Укажи имя агента: ./agent-worktree.sh --merge <имя>}"
  branch="agent/${name}"
  wt="$(dirname "$MAIN_ROOT")/${REPO_NAME}-${name}"
  target="$TARGET"

  # 1) ветка агента должна существовать
  if ! git -C "$MAIN_ROOT" show-ref --verify --quiet "refs/heads/${branch}"; then
    echo "✗ Ветки ${branch} нет — нечего вливать."; exit 1
  fi
  # 2) в папке агента не должно быть незакоммиченного (иначе оно НЕ попадёт в merge)
  if [[ -d "$wt" ]] && [[ -n "$(git -C "$wt" status --porcelain 2>/dev/null | grep -v 'frontend/node_modules' || true)" ]]; then
    echo "✗ В папке агента ($wt) есть незакоммиченные изменения — они НЕ попадут в слияние."
    echo "  Сначала закоммить их на ветке ${branch}, потом повтори --merge."
    exit 1
  fi

  echo "→ Обновляю ${target} с ${REMOTE}…"
  git -C "$MAIN_ROOT" fetch --prune "$REMOTE"

  # Сливаем в СВЕЖЕМ отдельном worktree на актуальном remote-${target}, чтобы не
  # зависеть от состояния основного каталога (там может быть чей-то WIP).
  tmp="$(dirname "$MAIN_ROOT")/.${REPO_NAME}-merge-tmp"
  rm -rf "$tmp"
  git -C "$MAIN_ROOT" worktree add --detach "$tmp" "${REMOTE}/${target}" >/dev/null

  echo "→ Вливаю ${branch} в ${target}…"
  set +e
  git -C "$tmp" merge --no-ff "$branch" -m "Merge ${branch} into ${target}"
  merge_rc=$?
  set -e
  if [[ $merge_rc -ne 0 ]]; then
    git -C "$tmp" merge --abort 2>/dev/null || true
    git -C "$MAIN_ROOT" worktree remove --force "$tmp"; git -C "$MAIN_ROOT" worktree prune
    echo "✗ КОНФЛИКТ при слиянии ${branch} → ${target}. Ничего не запушено."
    echo "  Реши вручную: cd \"$wt\" && git fetch $REMOTE && git rebase ${REMOTE}/${target}"
    echo "  (устрани конфликты, закоммить) и снова: ./agent-worktree.sh --merge ${name}"
    exit 1
  fi

  echo "→ Пушу ${target}…"
  set +e
  push_out="$(git -C "$tmp" push "$REMOTE" "HEAD:${target}" 2>&1)"; push_rc=$?
  set -e
  echo "$push_out" | tail -2
  git -C "$MAIN_ROOT" worktree remove --force "$tmp"; git -C "$MAIN_ROOT" worktree prune
  if [[ $push_rc -ne 0 ]]; then
    echo "✗ Пуш отклонён (в ${target} кто-то запушил параллельно). Просто повтори:"
    echo "    ./agent-worktree.sh --merge ${name}"
    exit 1
  fi

  # Успех: убираем рабочий worktree агента и его ветку — работа уже в ${target}.
  if [[ -d "$wt" ]]; then
    rm -f "$wt/frontend/node_modules"
    git -C "$MAIN_ROOT" worktree remove --force "$wt"
  fi
  git -C "$MAIN_ROOT" worktree prune
  git -C "$MAIN_ROOT" branch -D "$branch" 2>/dev/null || true

  cat <<EOF

✓ Готово: ${branch} влита в ${target} и запушена на ${REMOTE} → Railway задеплоит сам.
  Worktree и ветка агента '${name}' убраны.
  В основном каталоге при желании подтяни слияние: git pull --rebase ${REMOTE} ${target}
EOF
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
