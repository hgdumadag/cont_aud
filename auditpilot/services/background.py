from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from django.conf import settings
from django.db import close_old_connections


_executor = None
_executor_lock = Lock()


def _get_executor() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        with _executor_lock:
            if _executor is None:
                _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='auditpilot-run')
    return _executor


def _run_in_background(run_id: int) -> None:
    close_old_connections()
    try:
        from auditpilot.services.pipeline import process_audit_run

        process_audit_run(run_id)
    finally:
        close_old_connections()


def submit_audit_run_processing(run_id: int):
    if not getattr(settings, 'AUDITPILOT_BACKGROUND_PROCESSING', True):
        from auditpilot.services.pipeline import process_audit_run

        return process_audit_run(run_id)
    return _get_executor().submit(_run_in_background, run_id)
