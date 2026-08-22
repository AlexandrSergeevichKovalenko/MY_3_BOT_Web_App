# -*- coding: utf-8 -*-
"""Кто из агентов чем занят — отчёт владельцу в бота, сам, без команды.

ЗАЧЕМ. 22.08.2026 владелец спросил: «кто это делает и как мне понять, что оно взято в
работу другим агентом?» Ответить было нечем: в каталоге живут шесть-семь сессий Claude,
рядом три десятка рабочих каталогов, и единственный источник правды — коммиты постфактум.
Агент, взявший дефект, ничем себя не обозначает. Молчащий механизм неотличим от
сломанного — это ровно то, что запрещено правилом «мы всё автоматизируем».

Решение владельца 22.08.2026: отчёт приходит САМ в бота, ничего вызывать не нужно.

ОТКУДА БЕРУТСЯ ДАННЫЕ — четыре источника, все местные, ни одного вымышленного числа:

  1. ~/.claude/projects/<проект>/*.jsonl — расшифровки сессий. Оттуда «чем занята»:
     последняя просьба владельца в этой сессии и время последней записи.
  2. /tmp/cc-socks/*.sock + lsof — какие процессы Claude живы прямо сейчас и в каком
     каталоге сидят. Имя сокета — это PID.
  3. git worktree list + git status — что правится прямо сейчас и ещё не закоммичено.
  4. git log --all --since — что доведено до коммита за период.

ЧЕГО ЗДЕСЬ НЕТ И ПОЧЕМУ. Нет привязки «этот процесс = эта расшифровка»: надёжного
способа связать PID с файлом расшифровки нет, а связать «примерно по времени» значило бы
подписать чужую работу чужим именем. Поэтому живые процессы считаются отдельно, а занятия
показываются отдельно — и в отчёте это сказано словами, а не замазано.

ЕСЛИ ИСТОЧНИК МОЛЧИТ — отчёт падает с ошибкой и пишет её в лог, а не отправляет
половинчатую картину: «сессий 0» из-за сломанного lsof неотличимо от «все закончили».

    python3 scripts/agents_status_report.py            # напечатать
    python3 scripts/agents_status_report.py --send     # отправить владельцу в бота
    python3 scripts/agents_status_report.py --hours 3  # окно для коммитов, по умолчанию 1
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Владелец. Тот же номер, что у экономического отчёта (backend_server.py:1435).
OWNER_TELEGRAM_ID = 117649764

REPO = Path(__file__).resolve().parent.parent
TRANSCRIPTS = Path.home() / ".claude" / "projects" / "-Users-alexandr-Desktop-TELEGRAM-BOT-DEUTSCHESPRACHE"
SOCKETS = Path("/tmp/cc-socks")

# Сессия считается живой, если в её расшифровку писали за последние столько минут.
LIVE_MINUTES = 15
# Правка, которая лежит незакоммиченной дольше этого, — повод спросить «оно точно живо?».
STALE_HOURS = 12
# Сколько строк показываем в каждом разделе, прежде чем свернуть в «и ещё N».
MAX_ROWS = 8


def _git(*args: str, cwd: Path | str = REPO) -> str:
    """Git без глушения ошибок: не смог — падаем, а не показываем пустоту как правду."""
    done = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} → {done.returncode}: {done.stderr.strip()}")
    return done.stdout


def human_ago(seconds: float) -> str:
    """«3 мин назад», «2 ч назад», «4 дн назад» — время для человека, не для машины."""
    minutes = int(seconds // 60)
    if minutes < 1:
        return "только что"
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    return f"{hours // 24} дн назад"


def last_owner_request(path: Path) -> str:
    """Последняя просьба владельца в этой сессии — по ней видно, чем сессия занята.

    В расшифровке лежат и служебные записи: результаты вызовов, напоминания системы,
    вывод локальных команд. Они не просьба владельца, и подписывать ими сессию нельзя.
    """
    found, short = "", ""
    with path.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line or '"type":"user"' not in line.replace(" ", ""):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("type") != "user":
                continue
            content = (row.get("message") or {}).get("content")
            if isinstance(content, list):
                parts = [c.get("text", "") for c in content
                         if isinstance(c, dict) and c.get("type") == "text"]
                content = " ".join(p for p in parts if p)
            if not isinstance(content, str) or not content.strip():
                continue
            text = " ".join(content.split())
            # Всё, что начинается с угловой скобки, — служебная запись harness'а
            # (напоминание системы, доклад фоновой задачи, письмо соседней сессии,
            # вывод слэш-команды), а не слова владельца. Подписать ими сессию значило бы
            # выдать техническую строку за задачу.
            if text.startswith("<") or text.startswith("[Request interrupted"):
                continue
            # «ok», «да», «продолжай» — это ответ в разговоре, а не занятие сессии.
            # Держим их отдельно и показываем, только если содержательного нет вообще.
            if len(text) < 15:
                short = text
                continue
            found = text
    return found or short


def live_sessions() -> list[dict]:
    """Сессии, в расшифровку которых писали недавно, — с их занятием."""
    if not TRANSCRIPTS.is_dir():
        raise RuntimeError(f"нет каталога расшифровок: {TRANSCRIPTS}")
    now = time.time()
    rows = []
    for path in TRANSCRIPTS.glob("*.jsonl"):
        idle = now - path.stat().st_mtime
        if idle > LIVE_MINUTES * 60:
            continue
        rows.append({
            "id": path.stem[:8],
            "idle": idle,
            "task": last_owner_request(path),
        })
    return sorted(rows, key=lambda r: r["idle"])


def live_processes() -> list[dict]:
    """Живые процессы Claude и каталог, в котором каждый сидит. Имя сокета — это PID."""
    if not SOCKETS.is_dir():
        raise RuntimeError(f"нет каталога сокетов: {SOCKETS}")
    rows = []
    for sock in SOCKETS.glob("*.sock"):
        pid = sock.stem
        done = subprocess.run(["lsof", "-a", "-p", pid, "-d", "cwd", "-Fn"],
                              capture_output=True, text=True)
        cwd = ""
        for line in done.stdout.splitlines():
            if line.startswith("n"):
                cwd = line[1:]
        if cwd:
            rows.append({"pid": pid, "dir": Path(cwd).name})
    return rows


def dirty_worktrees() -> list[dict]:
    """Что правится прямо сейчас и ещё не закоммичено — по каждому рабочему каталогу."""
    trees = [line.split(" ", 1)[1].strip()
             for line in _git("worktree", "list", "--porcelain").splitlines()
             if line.startswith("worktree ")]
    now = time.time()
    rows = []
    for tree in trees:
        changed = [ln[3:] for ln in _git("status", "--porcelain", cwd=tree).splitlines()
                   if ln[:2] != "??"]
        if not changed:
            continue
        newest, newest_name = 0.0, ""
        for name in changed:
            target = Path(tree) / name
            if target.exists() and target.stat().st_mtime > newest:
                newest, newest_name = target.stat().st_mtime, name
        rows.append({
            "dir": Path(tree).name,
            "count": len(changed),
            "file": newest_name,
            "idle": now - newest if newest else 0.0,
        })
    return sorted(rows, key=lambda r: r["idle"])


def fresh_commits(hours: int) -> list[str]:
    """Что доведено до коммита за окно. Один заголовок = одна работа, дубли по веткам сняты."""
    out = _git("log", "--all", f"--since={hours} hours ago", "--format=%s")
    seen, rows = set(), []
    for subject in out.splitlines():
        subject = subject.strip()
        if not subject or subject in seen:
            continue
        # Служебные записи git (autostash, index on …) — не работа агента.
        if subject.startswith(("index on ", "On ", "WIP on ")):
            continue
        seen.add(subject)
        rows.append(subject)
    return rows


def _fold(rows: list[str]) -> list[str]:
    """Длинный список сворачиваем ЯВНО: молчаливая обрезка читается как «это всё»."""
    if len(rows) <= MAX_ROWS:
        return rows
    return rows[:MAX_ROWS] + [f"…и ещё {len(rows) - MAX_ROWS}"]


def build_report(*, sessions, processes, dirty, commits, hours: int, now_label: str) -> str:
    lines = [f"📋 Кто чем занят — {now_label}", ""]

    same_dir = [p for p in processes if p["dir"] == REPO.name]
    lines.append(f"Живых сессий Claude: {len(processes)}")
    if len(same_dir) > 1:
        lines.append(f"⚠️ {len(same_dir)} из них сидят в ОДНОМ каталоге ({REPO.name}).")
        lines.append("   Правки перемешиваются: чужой черновик попадает в чужой коммит.")
        lines.append("   Каждому агенту — свой каталог: ./agent-worktree.sh <имя>")
    lines.append("")

    lines.append("Чем заняты (последняя просьба к сессии):")
    if not sessions:
        lines.append(f"   тихо: за {LIVE_MINUTES} мин никто ничего не писал")
    else:
        rows = [f"   • {human_ago(s['idle'])} — {s['task'][:110] or '(без текста)'}"
                for s in sessions]
        lines.extend(_fold(rows))
    lines.append("")

    lines.append("Правится сейчас, ещё не закоммичено:")
    if not dirty:
        lines.append("   ничего — всё, что начато, доведено до коммита")
    else:
        rows = []
        for d in dirty:
            mark = "  ⚠️ висит" if d["idle"] > STALE_HOURS * 3600 else ""
            rows.append(f"   • {d['dir']}: {d['count']} файл(ов), "
                        f"свежий — {d['file']} ({human_ago(d['idle'])}){mark}")
        lines.extend(_fold(rows))
    lines.append("")

    lines.append(f"Доведено до коммита за {hours} ч:")
    if not commits:
        lines.append("   ничего")
    else:
        lines.extend(_fold([f"   • {c}" for c in commits]))

    return "\n".join(lines)


def owner_bot_token() -> str:
    """Токен бота — из БОЕВОГО окружения Railway, сервис MY_3_BOT. Источник ровно один.

    Почему не из переменной оболочки. 22.08.2026 проверено: в ~/.zshrc лежит токен той же
    длины, но ДРУГОЙ — телеграм отвечает на него 401. Взять «тот, который найдётся» —
    это и есть молчаливая деградация: отчёт бы не приходил, а причина выглядела бы как
    «телеграм недоступен». Поэтому источник называется по имени и он один.
    """
    railway = shutil.which("railway")
    if not railway:
        raise RuntimeError("нет railway CLI — токен бота взять неоткуда")
    done = subprocess.run([railway, "variables", "--service", "MY_3_BOT", "--json"],
                          cwd=str(REPO), capture_output=True, text=True)
    if done.returncode != 0:
        raise RuntimeError(f"railway variables → {done.returncode}: {done.stderr.strip()[:200]}")
    token = json.loads(done.stdout).get("TELEGRAM_Deutsch_BOT_TOKEN")
    if not token:
        raise RuntimeError("в боевом окружении нет TELEGRAM_Deutsch_BOT_TOKEN")
    return token


def send_to_owner(text: str) -> None:
    """Отправка владельцу. Не смогли — падаем: тихо не отправить хуже, чем не отправить."""
    token = owner_bot_token()
    import requests  # noqa: PLC0415 — нужен только на отправке

    answer = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": OWNER_TELEGRAM_ID, "text": text,
              "disable_web_page_preview": True},
        timeout=20,
    )
    if answer.status_code != 200:
        raise RuntimeError(f"телеграм ответил {answer.status_code}: {answer.text[:200]}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Кто из агентов чем занят")
    parser.add_argument("--send", action="store_true", help="отправить владельцу в бота")
    parser.add_argument("--hours", type=int, default=1, help="окно для коммитов, часов")
    args = parser.parse_args()

    report = build_report(
        sessions=live_sessions(),
        processes=live_processes(),
        dirty=dirty_worktrees(),
        commits=fresh_commits(args.hours),
        hours=args.hours,
        now_label=time.strftime("%d.%m %H:%M"),
    )
    print(report)
    if args.send:
        send_to_owner(report)
        print("\n(отправлено владельцу)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
