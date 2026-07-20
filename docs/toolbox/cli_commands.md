# Toolbox — terminal commands for reading and searching our code

This file explains the terminal commands you keep seeing in the learning docs (`grep -n ...` and
friends): what they are, **where** you type them, the exact **syntax**, and a **runnable example on
our repo** for each. Nothing here changes code — these are all read-only tools for finding your way
around 1.8 MB of `bot_3.py` and a 100-file backend.

## 0. What a "terminal" is and where to type

The **terminal** (a.k.a. shell, command line) is a text window where you type commands and the
computer runs them. On your Mac open the app **Terminal** (or the terminal panel inside VS Code:
View → Terminal). Our shell is **zsh**.

Every terminal has a **current directory** (where you "are"). Commands act relative to it. Two
commands to orient yourself:

```zsh
pwd            # "print working directory" — shows where you are, e.g. /Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE
cd backend     # "change directory" into backend/. `cd ..` goes up one level. `cd` alone → home.
```

Before running the examples below, make sure you're in the repo root:

```zsh
cd /Users/alexandr/Desktop/TELEGRAM_BOT_DEUTSCHESPRACHE
```

**How to read a command's syntax:** the shape is `command  [options]  arguments`. Options
(a.k.a. flags) start with `-` (short, like `-n`) or `--` (long, like `--color`) and tweak behavior.
Arguments are what the command acts on (a pattern, a filename). Options can be combined: `-rn` means
`-r -n`.

## 1. `grep` — find text inside files (the one you'll use most)

`grep` searches for a **pattern** (a piece of text) and prints every matching line. This is how you
answer "where is this function / word in the code?" without reading files top to bottom.

Syntax:

```
grep  [options]  "pattern"  path
```

- `pattern` — the text to look for, in quotes.
- `path` — a file, or a directory (with `-r`), or `.` for "everything under here".

The flags that matter (learn these five):

| Flag | Meaning | Why you want it |
| --- | --- | --- |
| `-n` | print the **line number** of each match | so you can jump straight to `file:line` |
| `-r` | **recursive** — search all files under a directory | search the whole project at once |
| `-i` | case-**insensitive** | find `Translate` and `translate` together |
| `-w` | match **whole words** only | `-w cat` won't match `category` |
| `-A N` / `-B N` / `-C N` | print N lines **after** / **before** / around each match | see context, not just the one line |

Runnable examples on our repo:

```zsh
# Where is the function get_webapp_sentences defined? (-n gives the line number)
grep -n "def get_webapp_sentences" backend/backend_server.py
#   → 52322:def get_webapp_sentences():   ← now you know it's at line 52322

# Every place the whole project registers a Flask route:
grep -rn "@app.route" backend/backend_server.py | head
#   `head` (see §3) trims the output to the first 10 lines so it doesn't flood the screen.

# Find where a word is used ANYWHERE in the backend, case-insensitive:
grep -rin "initdata" backend/ | head

# See 3 lines of context around each match (understand the surrounding code):
grep -n -C 3 "hmac.compare_digest" backend/backend_server.py
```

Reading the output: each line is `filename:linenumber:the matching line`. With `-r` over a
directory you also get the filename; on a single file you get `linenumber:line`.

## 2. `rg` (ripgrep) — a faster grep (use it if installed)

`rg` does the same job as `grep -rn` but is much faster and searches recursively **by default**,
and automatically skips junk like `node_modules` and `.git`. Same idea, shorter to type:

```zsh
rg "def get_webapp_sentences"          # recursive by default, shows file:line automatically
rg -i "initdata" backend/              # -i = case-insensitive, same as grep
rg -t py "reserve_free_feature_usage"  # -t py = only search Python files
```

Check if you have it: `rg --version`. If "command not found", either use `grep -rn` instead or
install it with `brew install ripgrep`. For our huge files, `rg` is noticeably nicer.

## 3. Trimming and counting output: `head`, `tail`, `wc`, and the pipe `|`

The **pipe** `|` sends the output of one command as the input of the next. It's how you chain tools.

```zsh
grep -rn "@app.route" backend/backend_server.py | head        # first 10 matching lines only
grep -rn "@app.route" backend/backend_server.py | tail        # last 10
grep -rn "@app.route" backend/backend_server.py | wc -l       # COUNT how many matches (wc -l = count lines)
```

- `head` — first 10 lines (`head -n 20` for 20).
- `tail` — last 10 lines (`tail -n 50`, or `tail -f file` to follow a growing log live).
- `wc -l` — "word count, lines" → just prints a number. Great for "how many routes do we have?".

## 4. Listing and reading files: `ls`, `cat`, `less`

```zsh
ls backend/                 # list files in a directory
ls -la                      # -l = long (sizes, dates), -a = include hidden dotfiles
cat backend/README.md       # dump a whole (small) file to the screen
less backend/backend_server.py   # open a BIG file in a scroll viewer: arrows/PgUp/PgDn to move,
                                 # type "/pattern" then Enter to search, "n" for next, "q" to quit
```

Rule of thumb: `cat` for small files, `less` for big ones (never `cat bot_3.py` — it's 1.8 MB and
will flood your terminal). In your editor (VS Code) you can also just `Cmd+P` a filename and
`Ctrl+G` a line number — same idea as `file:line`.

## 5. `find` — locate files by name (not by content)

`grep` searches *inside* files; `find` searches *for* files by name/path.

```zsh
find . -name "*.jsx"                 # every .jsx file under the current dir (. = here)
find backend -name "test_*.py"       # all pytest test files in backend/
find . -name "*.md" -not -path "*/node_modules/*"   # all docs, skipping node_modules
```

`.` means "current directory", `-name "pattern"` matches the filename (`*` = any characters),
`-not -path "..."` excludes a folder.

## 6. Git commands for reading history (read-only)

You'll also see git used just to *understand* the code's past — safe, read-only:

```zsh
git log --oneline -10                 # last 10 commits, one line each
git log --oneline -- backend/database.py | head    # history of ONE file
git show <commit_hash>                # what a specific commit changed
git blame backend/backend_server.py -L 4021,4037   # who wrote lines 4021–4037 and when
git grep -n "def get_webapp_sentences"             # like grep but only over git-tracked files (fast)
```

(Commands that *change* things — `commit`, `push` — are covered in
[backend_interaction.md](backend_interaction.md), because those touch the live app.)

## 7. Self-check

1. You want the line number where `save_webapp_translation` is defined in `backend/database.py`.
   Write the exact `grep` command.
2. What does the `|` (pipe) do in `grep -rn "x" . | wc -l`, and what single number comes out?
3. Why use `less` instead of `cat` for `bot_3.py`? What key quits `less`?
4. Difference between `grep` and `find` in one sentence each.
