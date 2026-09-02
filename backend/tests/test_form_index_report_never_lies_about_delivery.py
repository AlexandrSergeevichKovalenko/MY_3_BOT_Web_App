# -*- coding: utf-8 -*-
"""Отчёт не имеет права сказать «отправлено», если он не дошёл ни до кого.

Поймано на живой проверке 02.09.2026. Токен оказался не боевым, Telegram ответил
401 Unauthorized, письмо не ушло НИКОМУ — а функция вернула {"ok": True, "sent": 0} и
пометила прогон «completed». То есть в понедельник отчёт мог бы не прийти, и никто бы об
этом не узнал: в журнале планировщика стояло бы «выполнено».

Владелец 19.08.2026: «молчащий механизм неотличим от сломанного». Тест держит это место:
адресаты есть, письмо не ушло — значит провал, и он назван словом.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend import lex_form_index_sweep as S  # noqa: E402


class _Reply:
    def __init__(self, status):
        self.status_code = status
        self.text = '{"ok":false,"error_code":%d}' % status


def _arrange(monkeypatch, admins, statuses):
    """Подставить админов и ответы Telegram; вернуть счётчик отправок."""
    calls = {"n": 0}

    def fake_post(url, **kwargs):
        calls["n"] += 1
        return _Reply(statuses[min(calls["n"] - 1, len(statuses) - 1)])

    monkeypatch.setattr(S.requests, "post", fake_post)
    monkeypatch.setattr(S, "build_form_index_report_text", lambda: "отчёт")
    monkeypatch.setenv("TELEGRAM_Deutsch_BOT_TOKEN", "тестовый-токен")
    import backend.database as database
    monkeypatch.setattr(database, "get_admin_telegram_ids", lambda: admins, raising=False)
    return calls


def test_ни_одному_админу_не_дошло_это_провал(monkeypatch):
    _arrange(monkeypatch, [111, 222], [401])
    result = S.send_form_index_report(force=True)
    assert result["ok"] is False
    assert result["sent"] == 0
    assert "401" in str(result.get("error"))


def test_дошло_хотя_бы_одному_это_успех_но_сбой_назван(monkeypatch):
    _arrange(monkeypatch, [111, 222], [200, 500])
    result = S.send_form_index_report(force=True)
    assert result["ok"] is True
    assert result["sent"] == 1
    assert result["failed"], "неудачный адресат обязан быть назван, а не потеряться"


def test_дошло_всем_это_чистый_успех(monkeypatch):
    _arrange(monkeypatch, [111, 222], [200])
    result = S.send_form_index_report(force=True)
    assert result["ok"] is True
    assert result["sent"] == 2
    assert not result["failed"]


def test_без_токена_не_врём_про_отправку(monkeypatch):
    _arrange(monkeypatch, [111], [200])
    monkeypatch.delenv("TELEGRAM_Deutsch_BOT_TOKEN", raising=False)
    result = S.send_form_index_report(force=True)
    assert result["ok"] is False
    assert result.get("sent", 0) == 0
