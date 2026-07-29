#!/bin/sh
# Ставит git-хуки из scripts/git-hooks в .git/hooks.
#
# Хуки не хранятся в репозитории (git их туда не кладёт), поэтому после клона
# или в новом worktree их нужно поставить этой командой один раз:
#   ./scripts/git-hooks/install.sh

set -e
REPO_ROOT=$(git rev-parse --show-toplevel)
HOOK_DIR=$(git rev-parse --git-path hooks)
SRC="$REPO_ROOT/scripts/git-hooks"

mkdir -p "$HOOK_DIR"
for hook in pre-push; do
    cp "$SRC/$hook" "$HOOK_DIR/$hook"
    chmod +x "$HOOK_DIR/$hook"
    echo "✅ установлен $hook → $HOOK_DIR/$hook"
done
