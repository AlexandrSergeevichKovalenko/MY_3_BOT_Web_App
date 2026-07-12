web: gunicorn -b 0.0.0.0:$PORT -k gthread --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-2} --max-requests ${GUNICORN_MAX_REQUESTS:-2000} --max-requests-jitter ${GUNICORN_MAX_REQUESTS_JITTER:-200} --timeout 300 --graceful-timeout 30 backend.web_service:app
worker: python -m backend.background_jobs
translation_check_worker: python -m backend.run_dramatiq_worker
scheduler: python -m backend.scheduler_service
