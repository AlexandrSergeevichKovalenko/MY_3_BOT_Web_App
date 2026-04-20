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
