"""
Scheduler entry-points for TTS jobs.

The TTS generation engine (thread pool, in-process queue, Google TTS,
billing hooks, R2 upload) stays in backend_server for now. This module
owns the scheduler-facing wrappers so that background_jobs.py actors
do not import backend_server directly.
"""


def run_tts_prewarm_scheduler_job() -> None:
    from backend.backend_server import _run_tts_prewarm_scheduler_job
    _run_tts_prewarm_scheduler_job()


def run_tts_generation_recovery_scheduler_job() -> None:
    from backend.backend_server import _run_tts_generation_recovery_scheduler_job
    _run_tts_generation_recovery_scheduler_job()


def run_tts_admin_alerts_scheduler_job() -> None:
    """Сторож озвучки: смотрит, не встала ли генерация, и пишет владельцу.

    Жил в планировщике веб-процесса, а 01.06.2026 у веба отобрали фоновые задачи
    (`backend/web_service.py`) — сторож молча перестал запускаться. Обнаружено
    23.08.2026: 31 запись висела с мая, слова у людей не озвучивались, и никто
    об этом не сообщил. Читает он теперь общую таблицу событий, а не память
    процесса, поэтому спокойно работает на воркере.
    """
    from backend.backend_server import _run_tts_admin_alerts_scheduler_job
    _run_tts_admin_alerts_scheduler_job()


def run_tts_admin_digest_scheduler_job() -> None:
    """Суточная сводка по озвучке владельцу. Раз в день, решение владельца 23.08.2026."""
    from backend.backend_server import _run_tts_admin_digest_scheduler_job
    _run_tts_admin_digest_scheduler_job()


def run_tts_voice_reconcile_scheduler_job() -> None:
    """Ночная сверка: озвучка под голосом, которого экран не просит, — это молчание
    у человека и оплаченный впустую синтез. Добор накопленного, потолок за ночь."""
    from backend.backend_server import _run_tts_voice_reconcile_scheduler_job
    _run_tts_voice_reconcile_scheduler_job()


def run_tts_prewarm_quota_control_scheduler_job() -> None:
    from backend.backend_server import _run_tts_prewarm_quota_control_scheduler_job
    _run_tts_prewarm_quota_control_scheduler_job()
