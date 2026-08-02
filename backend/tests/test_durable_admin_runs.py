"""Долгий ручной прогон переживает перезапуск и всё равно отчитывается.

02.08.2026 смена переменной на Railway погасила контейнер посреди пересбора: сделано было
244 слова из 252, итог не пришёл никому, а по остатку в базе это выглядело как «модель не
смогла разобрать 8 слов». Отметка «идёт» в свежем процессе означает ровно одно — прогон
оборвали, и его надо досчитать, а владельцу сказать, где он встал.
"""
import bot_3


class _Recorder:
    """Подменяет и хранилище состояния, и отправку сообщений."""

    def __init__(self, status="", state=None):
        self.status = status
        self.state = dict(state or {})
        self.saved = []
        self.messages = []
        self.started = []

    def load(self, job_key):
        return self.status, dict(self.state)

    def save(self, job_key, status, state):
        self.status = status
        self.state = dict(state)
        self.saved.append((job_key, status, dict(state)))

    def send(self, chat_id, text):
        self.messages.append((chat_id, text))

    def start(self, **kwargs):
        self.started.append(kwargs)


def _install(monkeypatch, rec, only_job="admin_run_resweep_units"):
    monkeypatch.setattr(bot_3, "_admin_run_load", rec.load)
    monkeypatch.setattr(bot_3, "_admin_run_save", rec.save)
    monkeypatch.setattr(bot_3, "_admin_run_send", rec.send)
    monkeypatch.setattr(bot_3, "_start_durable_admin_run", rec.start)
    monkeypatch.setattr(
        bot_3, "_DURABLE_ADMIN_RUNS",
        {only_job: dict(bot_3._DURABLE_ADMIN_RUNS[only_job])},
    )


def test_interrupted_run_is_resumed_from_the_remainder(monkeypatch):
    rec = _Recorder("running", {"chat_id": 42, "limit": 300, "done": 244, "resumes": 0})
    _install(monkeypatch, rec)

    bot_3._resume_interrupted_admin_runs()

    assert len(rec.started) == 1
    started = rec.started[0]
    assert started["done_before"] == 244
    assert started["limit"] == 56          # остаток потолка, а не весь потолок заново
    assert started["resumes"] == 1
    assert started["chat_id"] == 42
    # владелец узнаёт, что прогон оборвался, ДО того как что-то снова закрутится
    assert rec.messages and "244" in rec.messages[0][1]


def test_finished_run_is_not_restarted(monkeypatch):
    rec = _Recorder("completed", {"chat_id": 42, "limit": 300, "done": 300})
    _install(monkeypatch, rec)

    bot_3._resume_interrupted_admin_runs()

    assert rec.started == []
    assert rec.messages == []


def test_exhausted_budget_closes_instead_of_running_again(monkeypatch):
    """Потолок выбран — продолжать нечего, но молчать нельзя: итог всё равно нужен."""
    rec = _Recorder("running", {"chat_id": 42, "limit": 250, "done": 250, "resumes": 0})
    _install(monkeypatch, rec)

    bot_3._resume_interrupted_admin_runs()

    assert rec.started == []
    assert rec.status == "completed"
    assert rec.messages and "продолжать нечего" in rec.messages[0][1]


def test_restart_loop_stops_burning_money(monkeypatch):
    """Если сервис перезапускается по кругу, досчёт обязан сдаться, а не платить вечно."""
    rec = _Recorder(
        "running",
        {"chat_id": 42, "limit": 300, "done": 10, "resumes": bot_3._ADMIN_RUN_MAX_RESUMES},
    )
    _install(monkeypatch, rec)

    bot_3._resume_interrupted_admin_runs()

    assert rec.started == []
    assert rec.status == "failed"
    assert rec.messages and "больше не продолжаю" in rec.messages[0][1]


def test_run_without_ceiling_resumes_without_one(monkeypatch):
    """Потолок не задавали — работа берёт свой собственный; дублировать его здесь нельзя."""
    rec = _Recorder("running", {"chat_id": 42, "limit": None, "done": 7, "resumes": 0})
    _install(monkeypatch, rec)

    bot_3._resume_interrupted_admin_runs()

    assert len(rec.started) == 1
    assert rec.started[0]["limit"] is None
    assert rec.started[0]["done_before"] == 7


def test_every_registered_run_can_be_resumed():
    """Новую долгую команду легко подключить и забыть про досчёт — тогда она снова
    потеряет итог при перезапуске. Реестр обязан быть полным."""
    for job_key, spec in bot_3._DURABLE_ADMIN_RUNS.items():
        assert callable(spec.get("runner")), job_key
        assert callable(spec.get("formatter")), job_key
        assert str(spec.get("title") or "").strip(), job_key
