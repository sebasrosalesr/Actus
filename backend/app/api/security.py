from __future__ import annotations

import os

from fastapi import HTTPException, Request


def _env_name() -> str:
    value = os.environ.get("ACTUS_ENV", "").strip().lower()
    return value or "development"


def is_production() -> bool:
    return _env_name() in {"production", "prod"}


def client_safe_detail(detail: str, *, generic: str) -> str:
    return generic if is_production() else detail


def should_log_tracebacks() -> bool:
    raw = os.environ.get("ACTUS_LOG_TRACEBACKS", "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return not is_production()


def current_api_key() -> str:
    return os.environ.get("ACTUS_API_KEY", "").strip()


def extract_api_key(request: Request) -> str | None:
    provided = request.headers.get("x-api-key")
    if provided:
        provided = provided.strip()
    if provided:
        return provided

    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1].strip()
        return token or None
    return None


def require_api_key(request: Request) -> None:
    expected = current_api_key()
    if not expected:
        return

    provided = extract_api_key(request)
    if not provided or provided != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
