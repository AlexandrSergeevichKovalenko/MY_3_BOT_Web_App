# -*- coding: utf-8 -*-
"""Перерисованная картинка обязана ДОЕХАТЬ до человека.

Повод (31.08.2026): картинку «Ei» починили 13.06.2026, правильный файл лежит в R2 с
03.08.2026 — а 31.08.2026 двенадцати людям снова ушла июньская груша под подписью
«яйцо + часы». Причина: адрес картинки собирается из номера ребуса и не меняется,
а Telegram кеширует картинку по адресу и второй раз за ней не приходит.

Эти тесты держат три правила:
  1. Другие байты — другой адрес.
  2. Склейка, разошедшаяся со своими половинками, НЕ отправляется.
  3. Судья, который сам не смог посмотреть, не говорит «годится».
"""

import sys
import types
from unittest.mock import patch

import pytest


def _r2_env(monkeypatch):
    monkeypatch.setenv("R2_ACCOUNT_ID", "acct")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("R2_BUCKET_NAME", "bucket")
    monkeypatch.setenv("R2_ENDPOINT", "https://ACCOUNT_ID.r2.example")
    monkeypatch.setenv("R2_PUBLIC_BASE_URL", "https://pub.example")


def test_same_key_different_bytes_gives_different_url(monkeypatch):
    """Ключ детерминирован (rebus/composed/<id>.png), поэтому перерисовка ложится в
    ТОТ ЖЕ адрес. Версия содержимого — единственное, что делает адрес новым."""
    from backend.r2_storage import (
        load_r2_config_from_env, r2_public_url, r2_content_version,
    )
    _r2_env(monkeypatch)
    load_r2_config_from_env.cache_clear() if hasattr(load_r2_config_from_env, "cache_clear") else None

    key = "rebus/composed/eieruhr_001.png"
    june = r2_content_version(b"pear-and-clock")
    august = r2_content_version(b"egg-and-clock")
    assert june != august, "разные байты обязаны давать разные версии"

    url_june = r2_public_url(key, version=june)
    url_august = r2_public_url(key, version=august)
    assert url_june != url_august, (
        "адрес не изменился при смене содержимого — Telegram отдаст свою старую копию"
    )
    assert url_august.endswith(august)
    # Пустая версия = «не знаем», и это честный голый адрес, а не выдуманное значение.
    assert "?" not in r2_public_url(key, version="")


def test_object_key_survives_the_version_in_the_url(monkeypatch):
    """Обратное преобразование обязано отбросить `?v=…`: иначе удаление объекта по
    сохранённой ссылке промахнётся мимо файла."""
    from backend.r2_storage import r2_public_url, r2_key_from_public_url
    _r2_env(monkeypatch)
    key = "rebus/composed/eieruhr_001.png"
    assert r2_key_from_public_url(r2_public_url(key, version="abc123")) == key


def test_card_whose_parts_changed_is_not_delivered():
    """Ровно случай «груша под словом яйцо»: части починили, склейку не пересобрали.
    Отпечаток расходится — карточка НЕ уходит."""
    from backend import rebus_generator

    entry = {
        "compound_id": "eieruhr_001",
        "composed_image_object_key": "rebus/composed/eieruhr_001.png",
        "composed_image_version": "v-june",
        # Склеена была из Birne+Uhr…
        "parts_fingerprint": "Birne:b1|Uhr:u1",
        # …а части в базе уже Ei+Uhr.
        "parts": [{"word": "Ei"}, {"word": "Uhr"}],
    }
    with patch.object(rebus_generator, "rebus_parts_fingerprint", return_value="Ei:e1|Uhr:u1"):
        assert rebus_generator.rebus_card_delivery_url(entry) == "", (
            "склейка разошлась с частями, а карточка всё равно ушла человеку"
        )


def test_card_without_fingerprint_is_not_delivered():
    """Отпечатка нет — сверить не с чем. «Не знаем» не равно «годится»."""
    from backend import rebus_generator

    entry = {
        "compound_id": "eieruhr_001",
        "composed_image_object_key": "rebus/composed/eieruhr_001.png",
        "composed_image_version": "v1",
        "parts_fingerprint": "",
        "parts": [{"word": "Ei"}, {"word": "Uhr"}],
    }
    with patch.object(rebus_generator, "rebus_parts_fingerprint", return_value="Ei:e1|Uhr:u1"):
        assert rebus_generator.rebus_card_delivery_url(entry) == ""


def test_matching_card_is_delivered_with_its_version(monkeypatch):
    from backend import rebus_generator
    from backend.r2_storage import load_r2_config_from_env
    _r2_env(monkeypatch)
    if hasattr(load_r2_config_from_env, "cache_clear"):
        load_r2_config_from_env.cache_clear()

    entry = {
        "compound_id": "eieruhr_001",
        "composed_image_object_key": "rebus/composed/eieruhr_001.png",
        "composed_image_version": "v-august",
        "parts_fingerprint": "Ei:e1|Uhr:u1",
        "parts": [{"word": "Ei"}, {"word": "Uhr"}],
    }
    with patch.object(rebus_generator, "rebus_parts_fingerprint", return_value="Ei:e1|Uhr:u1"):
        url = rebus_generator.rebus_card_delivery_url(entry)
    assert url.endswith("?v=v-august"), url


def test_vision_judge_that_could_not_look_does_not_say_ok(monkeypatch):
    """До 31.08.2026 судья при своей поломке отвечал ok=True, и непросмотренная
    картинка входила в банк как проверенная. Теперь это «не знаю»."""
    from backend import openai_manager

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    verdict = openai_manager.run_image_depicts(b"\x89PNG-bytes", "Ei", meaning="яйцо")
    assert verdict.get("ok") is False, "судья без ключа не имеет права говорить «годится»"
    assert verdict.get("unknown") is True, "«не смог посмотреть» обязано отличаться от «забраковал»"
