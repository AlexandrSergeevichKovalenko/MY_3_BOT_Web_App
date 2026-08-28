# -*- coding: utf-8 -*-
"""«Готово» на экране проверки слов ОТПУСКАЕТ человека, а не держит его на экране.

ЧТО СЛОМАЛОСЬ, 28.08.2026. Решения применялись прямо в HTTP-запросе, а на каждую
принятую правку фразы сервер синхронно ходит к модели пересобирать разбор. Замер по
живой базе в тот день: 30 фраз применялись 295 секунд, по ~10 секунд на каждую, при
лимите воркера 300 секунд (Procfile, Dockerfile.backend). То есть ответ до человека не
доходил В ПРИНЦИПЕ: экран навсегда оставался в «Сохраняю…», и владелец нажал «Готово»
второй раз — в базе это видно двумя заходами, 19:39 и 19:48.

Побочно: web-сервис держит один воркер на два потока, так что одно такое сохранение на
пять минут съедало половину всего сервиса.

Владелец 28.08.2026: «мы пользователя отпускаем, работу делаем под капотом».

ЧТО ЗДЕСЬ ЗАКРЕПЛЕНО:
  1. эндпоинт НЕ применяет решения сам — он их только ставит в очередь;
  2. очереди нет — человеку говорят правду, а не применяют молча синхронно
     (это вернуло бы пятиминутный экран) и не отвечают «готово» впустую;
  3. ни одной нажатой кнопки — в очередь не идём вовсе: сообщение «готово» на пустом
     месте это обещание без содержания.
"""
import pytest

from backend import backend_server


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(backend_server, "_telegram_hash_is_valid", lambda *a, **k: True)
    monkeypatch.setattr(
        backend_server, "_parse_telegram_init_data", lambda *a, **k: {"user": {"id": 777}}
    )
    return backend_server.app.test_client()


РЕШЕНИЯ = [
    {"word": "Sonnen sich", "kind": "phrase", "review_id": 332, "action": "fixed",
     "variant_text": "Sich sonnen"},
    {"word": "Vorrücker", "kind": "word", "action": "keep"},
    {"word": "нетронутое", "kind": "word", "action": ""},
]


def _не_применять(monkeypatch):
    """Сторож: если применение всё-таки поедет в запросе, тест упадёт по делу."""
    from backend import word_confirm_digest

    def взрыв(*a, **k):
        raise AssertionError("решения применяются В ЗАПРОСЕ — человек снова ждёт")

    monkeypatch.setattr(word_confirm_digest, "apply_decisions", взрыв)


def test_decisions_are_queued_and_the_person_is_released(client, monkeypatch):
    _не_применять(monkeypatch)
    поставлено = {}
    from backend import job_queue

    def очередь(*, user_id, decisions):
        поставлено["user_id"] = user_id
        поставлено["decisions"] = decisions
        return "msg-1"

    monkeypatch.setattr(job_queue, "enqueue_word_audit_apply_job", очередь)

    ответ = client.post("/api/webapp/word-audit/apply",
                        json={"initData": "x", "decisions": РЕШЕНИЯ})
    тело = ответ.get_json()
    assert ответ.status_code == 200
    assert тело["ok"] is True and тело["queued"] is True
    # Отвечаем числом ЕГО решений, а не итогом работы: итога ещё нет.
    assert тело["accepted"] == 2
    assert поставлено["user_id"] == 777
    # Неотмеченное до очереди не едет: в фоне ему нечего делать.
    assert [d["word"] for d in поставлено["decisions"]] == ["Sonnen sich", "Vorrücker"]


def test_nothing_marked_means_nothing_queued(client, monkeypatch):
    _не_применять(monkeypatch)
    from backend import job_queue

    def очередь(*, user_id, decisions):
        raise AssertionError("пустые решения ушли в очередь")

    monkeypatch.setattr(job_queue, "enqueue_word_audit_apply_job", очередь)

    ответ = client.post("/api/webapp/word-audit/apply",
                        json={"initData": "x",
                              "decisions": [{"word": "нетронутое", "action": ""}]})
    тело = ответ.get_json()
    assert тело["ok"] is True and тело["queued"] is False and тело["accepted"] == 0


def test_no_queue_means_an_honest_no_not_a_silent_five_minute_wait(client, monkeypatch):
    _не_применять(monkeypatch)
    from backend import job_queue

    def нет_очереди(*, user_id, decisions):
        raise RuntimeError("background_jobs_unavailable")

    monkeypatch.setattr(job_queue, "enqueue_word_audit_apply_job", нет_очереди)

    ответ = client.post("/api/webapp/word-audit/apply",
                        json={"initData": "x", "decisions": РЕШЕНИЯ})
    тело = ответ.get_json()
    assert тело["ok"] is False
    assert "Ничего не потеряно" in тело["message"]


def test_the_background_report_names_what_was_done(monkeypatch):
    """Человек ушёл с экрана — итог он увидит только сообщением. Молчать нельзя."""
    from backend.background_jobs import _word_audit_report_text

    текст = _word_audit_report_text({"исправлено": 36, "оставлено": 2, "удалено": 0})
    assert "исправлено: 36" in текст
    assert "удалено" not in текст          # нулей человеку не показываем
    assert _word_audit_report_text({}).startswith("🦊")
