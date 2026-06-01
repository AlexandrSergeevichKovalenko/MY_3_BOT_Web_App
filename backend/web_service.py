"""
Dedicated web service entrypoint.

This wrapper keeps the web process lean by disabling backend runtime side
effects before importing the Flask app. Background workers and scheduler
processes have their own dedicated entrypoints.
"""

import os


os.environ["BACKEND_RUNTIME_SIDE_EFFECTS_ENABLED"] = "0"

from backend.backend_server import app  # noqa: E402


__all__ = ["app"]
