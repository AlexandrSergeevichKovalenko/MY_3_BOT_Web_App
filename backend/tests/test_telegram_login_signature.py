# -*- coding: utf-8 -*-
"""Вход через виджет Telegram: лишнее поле в теле не ломает подпись, подделка ломается.

Владелец 23.08.2026 впервые дошёл до экрана входа — и увидел там
{"error":"Telegram login hash invalid"}. Оказалось, вход через виджет не работал НИКОГДА;
это не замечали, потому что приложение держало сохранённый пропуск и логин не требовался.

ПРИЧИНА БЫЛА В НАШЕМ КОДЕ. У автономного приложения есть перехватчик запросов
(frontend/src/main.jsx): он дописывает свой токен полем `aqt` в тело каждого POST к /api/.
Проверка подписи считала хеш по ВСЕМ полям тела, включая дописанное, — а подпись Telegram
его не покрывает. Хеш не сходился ни разу.

Починка с двух сторон: сервер считает подпись только по полям, которые подписал Telegram,
а перехватчик не трогает рукопожатие входа вовсе. Здесь проверяется серверная половина —
она обязана держать и без второй.
"""
import hashlib
import hmac
import time

import pytest

import backend.backend_server as bs

TOKEN = "123456:test-token-not-real"


def _signed(**extra) -> dict:
    """Посылка, подписанная РОВНО так, как её подписывает виджет Telegram."""
    payload = {
        "id": 117649764,
        "first_name": "Alex",
        "username": "someone",
        "auth_date": int(time.time()),
    }
    payload.update({k: v for k, v in extra.items() if k != "hash"})
    signed = {k: v for k, v in payload.items() if k in bs._TELEGRAM_LOGIN_SIGNED_FIELDS}
    check = "\n".join(f"{k}={signed[k]}" for k in sorted(signed))
    secret = hashlib.sha256(TOKEN.encode()).digest()
    payload["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return payload


@pytest.fixture(autouse=True)
def _token(monkeypatch):
    monkeypatch.setattr(bs, "_ensure_telegram_bot_token", lambda: TOKEN)


class TestЛишнееПолеНеЛомаетВход:
    def test_дописанный_токен_приложения(self):
        """Ровно случай владельца: перехватчик дописал в тело `aqt`."""
        assert bs._telegram_login_hash_is_valid({**_signed(), "aqt": "app-token-abc"})

    def test_любое_чужое_поле(self):
        assert bs._telegram_login_hash_is_valid({**_signed(), "что_угодно": "x"})

    def test_чистая_посылка_как_прежде(self):
        assert bs._telegram_login_hash_is_valid(_signed())

    def test_необязательные_поля_участвуют_в_подписи(self):
        """last_name и photo_url Telegram подписывает — значит и мы обязаны их считать."""
        payload = _signed(last_name="Кова", photo_url="https://t.me/i/1.jpg")
        assert bs._telegram_login_hash_is_valid(payload)


class TestПодделкаНеПроходит:
    def test_чужая_подпись(self):
        assert not bs._telegram_login_hash_is_valid({**_signed(), "hash": "0" * 64})

    def test_подменённый_id(self):
        """Главное, ради чего подпись и нужна: войти под чужим номером нельзя."""
        payload = _signed()
        payload["id"] = 999
        assert not bs._telegram_login_hash_is_valid(payload)

    def test_подменённое_имя(self):
        payload = _signed()
        payload["first_name"] = "Кто-то другой"
        assert not bs._telegram_login_hash_is_valid(payload)

    def test_подпись_другого_бота(self):
        payload = _signed()
        secret = hashlib.sha256(b"999:other-bot").digest()
        check = "\n".join(f"{k}={payload[k]}" for k in sorted(payload) if k != "hash")
        payload["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        assert not bs._telegram_login_hash_is_valid(payload)

    def test_без_подписи(self):
        payload = _signed()
        payload.pop("hash")
        assert not bs._telegram_login_hash_is_valid(payload)

    def test_добавленное_поле_не_даёт_подделать_подписанное(self):
        """Лишние поля игнорируются — но подписанные всё так же проверяются."""
        payload = {**_signed(), "aqt": "app-token"}
        payload["username"] = "чужой"
        assert not bs._telegram_login_hash_is_valid(payload)


def test_перехватчик_не_трогает_рукопожатие_входа():
    """Вторая половина починки: клиент не должен дописывать ничего в тело входа."""
    import pathlib
    main = pathlib.Path(bs.__file__).resolve().parents[1] / "frontend" / "src" / "main.jsx"
    source = main.read_text(encoding="utf-8")
    assert "/api/web/auth/" in source, "перехватчик снова дописывает в тело входа"
