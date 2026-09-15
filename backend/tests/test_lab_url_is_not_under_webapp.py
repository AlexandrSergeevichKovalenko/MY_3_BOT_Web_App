"""Адрес страницы-лаборатории не должен попадать под перенаправление /webapp/ → /api/webapp/.

Повод (15.09.2026): команда /lab строила ссылку от get_webapp_url(), а тот возвращает
не корень сайта, а «<хост>/webapp». Получалось «…/webapp/lab/kbd.html»; backend_server
считает префикс /webapp/ устаревшим адресом API (_LEGACY_API_PREFIXES), перенаправляет
307-м на /api/webapp/lab/kbd.html, а там стоит страж initData. Владелец нажал кнопку в
боте и увидел голый JSON {"error":"initData обязателен"} вместо страницы.

Тест держит ДВА утверждения сразу, потому что поодиночке каждое обходится:
  1) путь лаборатории не начинается ни с одного устаревшего префикса API;
  2) команда строит ссылку от корня сайта, а не от точки входа мини-аппа.
"""
import os
import re
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _lab_path() -> str:
    src = (ROOT / "bot_3.py").read_text(encoding="utf-8")
    m = re.search(r'^LAB_KEYBOARD_PATH\s*=\s*"([^"]+)"', src, re.M)
    assert m, "LAB_KEYBOARD_PATH пропал из bot_3.py"
    return m.group(1)


def test_путь_лаборатории_не_под_устаревшими_префиксами_api():
    from backend.backend_server import _LEGACY_API_PREFIXES
    path = _lab_path()
    попал = [p for p in _LEGACY_API_PREFIXES if path.startswith(p)]
    assert not попал, (
        f"{path} начинается с {попал} — сервер перенаправит его в /api/... "
        f"и страж initData отдаст JSON вместо страницы")


def test_команда_строит_ссылку_от_корня_сайта_а_не_от_входа_в_мини_апп():
    src = (ROOT / "bot_3.py").read_text(encoding="utf-8")
    тело = src.split("async def _admin_lab_command", 1)
    assert len(тело) == 2, "команда /lab пропала из bot_3.py"
    тело = тело[1].split("\nasync def ", 1)[0]
    assert "get_public_web_url()" in тело, "ссылка должна строиться от корня сайта"
    # get_webapp_url() допустим только внутри пояснительного комментария-вердикта
    код = "\n".join(l for l in тело.split("\n") if not l.lstrip().startswith("#"))
    assert "get_webapp_url()" not in код, (
        "get_webapp_url() возвращает «<хост>/webapp» — ссылка уйдёт под перенаправление API")


def test_страница_лаборатории_лежит_в_исходниках():
    # Путь «/lab/kbd.html» обслуживается файлом frontend/public/lab/kbd.html: vite
    # копирует public/ в dist/ как есть, а Flask отдаёт существующий файл из dist.
    файл = ROOT / "frontend" / "public" / _lab_path().lstrip("/")
    assert файл.exists(), f"нет файла {файл} — кнопка поведёт на пустоту"


def test_lab_исключён_из_подмены_service_worker():
    # Без запрета service worker подменит страницу index.html'ом приложения из кеша.
    cfg = (ROOT / "frontend" / "vite.config.js").read_text(encoding="utf-8")
    m = re.search(r"navigateFallbackDenylist:\s*\[([^\]]*)\]", cfg)
    assert m, "navigateFallbackDenylist пропал из vite.config.js"
    assert "/lab" in m.group(1), "в navigateFallbackDenylist нет /lab"
