from __future__ import annotations

from collections import deque
import copy
import hashlib
import json
import math
import os
import time
import threading
from pathlib import Path
from typing import Any, Dict, Literal

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.responses import JSONResponse

import firebase_admin
from firebase_admin import credentials, db

from actus.auto_mode import auto_mode_answer
from actus.intent_router import actus_answer
from actus.openrouter_client import openrouter_chat
from scripts import build_rag_index

DEFAULT_OPENROUTER_FALLBACK_MODEL = "google/gemini-3.1-flash-lite-preview"
DEFAULT_DEV_ALLOWED_HOSTS = ["localhost", "127.0.0.1", "[::1]", "testserver"]
CORS_ALLOWED_METHODS = ["GET", "POST", "OPTIONS"]
CORS_ALLOWED_HEADERS = ["Content-Type", "Authorization", "X-API-Key"]


# Local .env should fill missing values, not override deployed environment.
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)


def _env_name() -> str:
    value = os.environ.get("ACTUS_ENV", "").strip().lower()
    return value or "development"


def _is_production() -> bool:
    return _env_name() in {"production", "prod"}


def _flag_enabled(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _docs_enabled() -> bool:
    return _flag_enabled("ACTUS_DOCS_ENABLED", default=not _is_production())


def _auth_mode() -> str:
    raw = os.environ.get("ACTUS_AUTH_MODE", "").strip().lower()
    if raw:
        return raw
    return "api_key" if os.environ.get("ACTUS_API_KEY", "").strip() else "none"


def _cors_origins() -> list[str]:
    raw = os.environ.get("ACTUS_CORS_ORIGINS", "")
    if raw.strip():
        return [origin.strip() for origin in raw.split(",") if origin.strip()]
    return ["http://localhost:5173", "http://127.0.0.1:5173"]


def _cors_origin_regex() -> str | None:
    raw = os.environ.get("ACTUS_CORS_ORIGIN_REGEX", "").strip()
    return raw or None


def _trusted_hosts() -> list[str]:
    raw = os.environ.get("ACTUS_ALLOWED_HOSTS", "")
    if raw.strip():
        return [host.strip() for host in raw.split(",") if host.strip()]
    return list(DEFAULT_DEV_ALLOWED_HOSTS)


def _validate_runtime_security() -> None:
    if not _is_production():
        return

    errors: list[str] = []
    auth_mode = _auth_mode()
    if auth_mode == "api_key":
        if not os.environ.get("ACTUS_API_KEY", "").strip():
            errors.append("ACTUS_API_KEY is required in production when ACTUS_AUTH_MODE=api_key.")
    elif auth_mode == "public":
        if not os.environ.get("ACTUS_API_KEY", "").strip():
            errors.append("ACTUS_API_KEY is required in production when ACTUS_AUTH_MODE=public for internal routes.")
    elif auth_mode == "cloudflare_access":
        if not os.environ.get("ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN", "").strip():
            errors.append("ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN is required in production when ACTUS_AUTH_MODE=cloudflare_access.")
        if not os.environ.get("ACTUS_CLOUDFLARE_ACCESS_AUD", "").strip():
            errors.append("ACTUS_CLOUDFLARE_ACCESS_AUD is required in production when ACTUS_AUTH_MODE=cloudflare_access.")
    else:
        errors.append("ACTUS_AUTH_MODE must be one of: public, api_key, cloudflare_access.")
    if not os.environ.get("ACTUS_CORS_ORIGINS", "").strip():
        errors.append("ACTUS_CORS_ORIGINS is required in production.")
    if os.environ.get("ACTUS_CORS_ORIGIN_REGEX", "").strip():
        errors.append("ACTUS_CORS_ORIGIN_REGEX is not allowed in production; use explicit ACTUS_CORS_ORIGINS.")
    if _docs_enabled():
        errors.append("ACTUS_DOCS_ENABLED must be false in production.")
    if not os.environ.get("ACTUS_ALLOWED_HOSTS", "").strip():
        errors.append("ACTUS_ALLOWED_HOSTS is required in production.")

    if errors:
        raise RuntimeError(
            "Insecure production configuration:\n- " + "\n- ".join(errors)
        )


_validate_runtime_security()

APP = FastAPI(
    title="Actus Backend",
    version="0.1.0",
    docs_url="/docs" if _docs_enabled() else None,
    redoc_url="/redoc" if _docs_enabled() else None,
    openapi_url="/openapi.json" if _docs_enabled() else None,
)

APP.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=_trusted_hosts(),
)

APP.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_origin_regex=_cors_origin_regex(),
    allow_credentials=True,
    allow_methods=CORS_ALLOWED_METHODS,
    allow_headers=CORS_ALLOWED_HEADERS,
)


@APP.middleware("http")
async def _api_key_guard(request: Request, call_next):
    if _auth_mode() != "api_key":
        return await call_next(request)

    api_key = os.environ.get("ACTUS_API_KEY")
    if not api_key:
        return await call_next(request)

    path = request.url.path or ""
    if path == "/api/health":
        return await call_next(request)
    if _docs_enabled() and (path.startswith("/docs") or path.startswith("/openapi") or path.startswith("/redoc")):
        return await call_next(request)

    if not (path.startswith("/api") or path.startswith("/rag")):
        return await call_next(request)

    provided = request.headers.get("x-api-key")
    if not provided:
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            provided = auth.split(" ", 1)[1].strip()

    if not provided or provided != api_key:
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)

    return await call_next(request)


@APP.middleware("http")
async def _rate_limit_guard(request: Request, call_next):
    if not _rate_limit_enabled():
        return await call_next(request)

    scope = _rate_limit_scope(request.url.path or "")
    if not scope:
        return await call_next(request)

    allowed, retry_after = _consume_rate_limit(scope, _request_identity(request))
    if not allowed:
        return JSONResponse(
            {"detail": "Rate limit exceeded"},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )

    return await call_next(request)

APP_DIR = Path(__file__).with_name("app")
if APP_DIR.is_dir():
    # Allow app.py to behave like a package for app.* imports.
    __path__ = [str(APP_DIR)]

from app.rag.runtime_env import ensure_openmp_env
ensure_openmp_env()

from app.api.rag import router as rag_router
from app.api.help import router as help_router
from app.api.quality import router as quality_router
from app.api.security import require_authenticated_request, require_internal_request
from app.quality.store import init_quality_db, record_quality_event, resolve_db_path
from app.rag.new_design.service import get_runtime_service
APP.include_router(rag_router)
APP.include_router(help_router)
APP.include_router(quality_router)

DATA_TTL_SEC = 120
_CACHE: Dict[str, Any] = {"df": None, "loaded_at": 0.0}
ASK_CACHE_TTL_SEC = 90
_ASK_CACHE: Dict[tuple[str, str, Any], Dict[str, Any]] = {}
_ASK_CACHE_LOCK = threading.Lock()
_RAG_REBUILD_STOP = threading.Event()
_RAG_PRELOAD_LOCK = threading.Lock()
_RAG_PRELOAD_STARTED = False
_RAG_WARMUP_LOCK = threading.Lock()
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_BUCKETS: Dict[tuple[str, str], deque[float]] = {}


class AskRequest(BaseModel):
    query: str
    mode: Literal["manual", "auto"] = "manual"


def _release_tag() -> str:
    value = os.environ.get("ACTUS_RELEASE_TAG", "").strip()
    return value or "dev"


def _client_safe_detail(detail: str, *, generic: str) -> str:
    return generic if _is_production() else detail


def _client_safe_error(detail: str, *, generic: str = "Request failed.") -> str:
    return generic if _is_production() else detail


def _log_raw_queries_enabled() -> bool:
    return _flag_enabled("ACTUS_LOG_RAW_QUERIES", default=not _is_production())


def _quality_query_value(query: str) -> str:
    text = str(query or "")
    if _log_raw_queries_enabled():
        return text
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _rate_limit_enabled() -> bool:
    return _flag_enabled("ACTUS_RATE_LIMIT_ENABLED", default=_is_production())


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def _rate_limit_window_sec() -> int:
    return _env_int("ACTUS_RATE_LIMIT_WINDOW_SEC", 60)


def _rate_limit_scope(path: str) -> str | None:
    normalized = str(path or "")
    if normalized == "/api/ask":
        return "ask"
    if normalized == "/api/health/openrouter":
        return "openrouter_health"
    if normalized.startswith("/rag"):
        return "rag"
    return None


def _rate_limit_limit(scope: str) -> int:
    if scope == "ask":
        return _env_int("ACTUS_RATE_LIMIT_ASK_PER_MIN", 30)
    if scope == "openrouter_health":
        return _env_int("ACTUS_RATE_LIMIT_OPENROUTER_HEALTH_PER_MIN", 5)
    if scope == "rag":
        return _env_int("ACTUS_RATE_LIMIT_RAG_PER_MIN", 20)
    return 0


def _request_identity(request: Request) -> str:
    provided = request.headers.get("x-api-key")
    if not provided:
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            provided = auth.split(" ", 1)[1].strip()
    if provided:
        digest = hashlib.sha256(provided.encode("utf-8")).hexdigest()[:16]
        return f"api_key:{digest}"

    forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    if forwarded:
        return f"ip:{forwarded}"

    client = getattr(request, "client", None)
    host = getattr(client, "host", None) if client else None
    return f"ip:{host or 'unknown'}"


def _consume_rate_limit(scope: str, identity: str) -> tuple[bool, int]:
    limit = _rate_limit_limit(scope)
    if limit <= 0:
        return True, 0

    now = time.monotonic()
    window = float(_rate_limit_window_sec())
    key = (scope, identity)
    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_BUCKETS.setdefault(key, deque())
        while bucket and (now - bucket[0]) >= window:
            bucket.popleft()
        if len(bucket) >= limit:
            retry_after = max(1, math.ceil(window - (now - bucket[0])))
            return False, retry_after
        bucket.append(now)
        return True, 0


def _infer_intent_id(query: str, meta: Dict[str, Any] | None) -> str | None:
    if isinstance(meta, dict):
        auto_mode = meta.get("auto_mode")
        if isinstance(auto_mode, dict) and auto_mode.get("enabled"):
            return "auto_mode"
        direct = meta.get("intent") or meta.get("intent_id")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        follow_up = meta.get("follow_up")
        if isinstance(follow_up, dict):
            value = follow_up.get("intent")
            if isinstance(value, str) and value.strip():
                return value.strip()
        if isinstance(meta.get("ticket_analysis"), dict):
            return "ticket_analysis"
        if isinstance(meta.get("item_analysis"), dict):
            return "item_analysis"
        if isinstance(meta.get("customer_analysis"), dict):
            return "customer_analysis"
        for key in ("creditTrends", "creditAnomalies", "creditOpsSnapshot"):
            if key in meta:
                return key

    normalized = query.strip().lower()
    if normalized.startswith("analyze ticket"):
        return "ticket_analysis"
    if normalized.startswith("analyze item"):
        return "item_analysis"
    if normalized.startswith("analyze account") or normalized.startswith("analyze customer"):
        return "customer_analysis"
    return None


def _safe_log_quality_event(
    *,
    query: str,
    rows: list[dict[str, Any]],
    meta: Dict[str, Any] | None,
    ok: bool,
    elapsed_ms: float,
    provider: str,
    error: str | None = None,
) -> None:
    payload = meta if isinstance(meta, dict) else {}
    ticket_analysis = payload.get("ticket_analysis")
    highlight_source = None
    highlight_model = None
    if isinstance(ticket_analysis, dict):
        highlight_source = ticket_analysis.get("investigation_highlights_source")
        highlight_model = ticket_analysis.get("investigation_highlights_model")

    compact_meta = {
        "keys": sorted(list(payload.keys()))[:40],
        "suggestion_count": len(payload.get("suggestions", []))
        if isinstance(payload.get("suggestions"), list)
        else 0,
        "query_length": len(str(query or "")),
        "query_logging": "raw" if _log_raw_queries_enabled() else "fingerprint",
    }
    auto_mode = payload.get("auto_mode")
    if isinstance(auto_mode, dict) and auto_mode.get("enabled"):
        executed_intents = auto_mode.get("executed_intents")
        compact_meta["auto_mode"] = True
        compact_meta["subintent_count"] = int(auto_mode.get("subintent_count") or 0)
        compact_meta["primary_intent"] = str(auto_mode.get("primary_intent") or "")
        compact_meta["executed_intents"] = [
            str(item.get("id") or "").strip()
            for item in (executed_intents if isinstance(executed_intents, list) else [])
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        ][:10]
    try:
        record_quality_event(
            {
                "query": _quality_query_value(query),
                "intent_id": _infer_intent_id(query, payload),
                "provider": provider,
                "latency_ms": elapsed_ms,
                "ok": ok,
                "error": _client_safe_error(error) if error else None,
                "result_count": len(rows),
                "has_ticket_analysis": isinstance(payload.get("ticket_analysis"), dict),
                "has_item_analysis": isinstance(payload.get("item_analysis"), dict),
                "highlight_source": highlight_source,
                "highlight_model": highlight_model,
                "is_help": bool(payload.get("is_help", False)),
                "release_tag": _release_tag(),
                "meta": compact_meta,
            }
        )
    except Exception as exc:
        print(f"[quality] log failed: {exc}")


def _ask_response(
    *,
    query: str,
    text: str,
    rows: list[dict[str, Any]] | None,
    meta: Dict[str, Any] | None,
    t0: float,
    ok: bool = True,
    provider: str = "actus",
    error: str | None = None,
) -> Dict[str, Any]:
    row_list = rows or []
    meta_obj = meta or {}
    elapsed_ms = (time.perf_counter() - t0) * 1000
    _safe_log_quality_event(
        query=query,
        rows=row_list,
        meta=meta_obj,
        ok=ok,
        elapsed_ms=elapsed_ms,
        provider=provider,
        error=error,
    )
    payload: Dict[str, Any] = {"text": text, "rows": row_list, "meta": meta_obj}
    if error:
        payload["error"] = _client_safe_error(error)
    return payload


def _openrouter_call(query: str) -> str:
    system_prompt = os.environ.get(
        "ACTUS_OPENROUTER_SYSTEM",
        "You are Actus, an operations copilot for credit and ticket analytics. Be concise and actionable.",
    )
    fallback_model = os.environ.get("ACTUS_OPENROUTER_MODEL_FALLBACK", "").strip() or DEFAULT_OPENROUTER_FALLBACK_MODEL

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": query},
    ]
    try:
        return openrouter_chat(messages)
    except Exception:
        if fallback_model:
            return openrouter_chat(messages, model=fallback_model)
        raise


def _ensure_firebase_app() -> None:
    firebase_json = os.environ.get("ACTUS_FIREBASE_JSON")
    firebase_path = os.environ.get("ACTUS_FIREBASE_PATH")

    if firebase_json:
        firebase_config = json.loads(firebase_json)
    elif firebase_path:
        with open(firebase_path, "r") as handle:
            firebase_config = json.load(handle)
    else:
        raise RuntimeError(
            "Missing Firebase credentials. Set ACTUS_FIREBASE_JSON or ACTUS_FIREBASE_PATH."
        )

    if "private_key" in firebase_config and "\\n" in firebase_config["private_key"]:
        firebase_config["private_key"] = firebase_config["private_key"].replace("\\n", "\n")

    database_url = os.environ.get(
        "ACTUS_FIREBASE_URL", "https://creditapp-tm-default-rtdb.firebaseio.com/"
    )

    if not firebase_admin._apps:
        cred = credentials.Certificate(firebase_config)
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": database_url},
        )


def _load_firebase_df() -> pd.DataFrame:
    _ensure_firebase_app()
    ref = db.reference("credit_requests")
    raw = ref.get() or {}
    raw_values = list(raw.values())
    df_ = pd.DataFrame(raw_values)
    rename_map = {
        "Invoice #": "Invoice Number",
        "Customer #": "Customer Number",
        "Item #": "Item Number",
    }
    df_ = df_.rename(columns=rename_map)
    expected_columns = [
        "Record ID",
        "Ticket Number",
        "Invoice Number",
        "Requested By",
        "Sales Rep",
        "Issue Type",
        "Date",
        "Status",
        "Reason for Credit",
        "RTN_CR_No",
        "Customer Number",
        "Item Number",
        "Credit Request Total",
    ]
    for col in expected_columns:
        if col not in df_.columns:
            df_[col] = None
    # Treat Firebase "Date" as a date-only value even if it includes a timestamp.
    raw_date = df_["Date"].astype(str).str.strip()
    date_part = raw_date.str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    date_series = pd.to_datetime(date_part.fillna(raw_date), errors="coerce")
    df_["Date"] = date_series.dt.normalize()
    return df_


def _get_df() -> pd.DataFrame:
    now = time.time()
    cached = _CACHE["df"]
    if cached is not None and now - _CACHE["loaded_at"] < DATA_TTL_SEC:
        if isinstance(getattr(cached, "attrs", None), dict):
            cached.attrs.setdefault("_actus_df_cache_token", round(float(_CACHE["loaded_at"]), 3))
        return cached

    df_ = _load_firebase_df()
    _CACHE["df"] = df_
    _CACHE["loaded_at"] = now
    if isinstance(getattr(df_, "attrs", None), dict):
        df_.attrs["_actus_df_cache_token"] = round(float(now), 3)
    return df_


def _ask_cache_enabled() -> bool:
    return _flag_enabled("ACTUS_ASK_CACHE_ENABLED", default=True)


def _ask_cache_ttl_sec() -> int:
    return _env_int("ACTUS_ASK_CACHE_TTL_SEC", ASK_CACHE_TTL_SEC)


def _df_cache_token(df: pd.DataFrame) -> Any:
    if _CACHE.get("df") is df and _CACHE.get("loaded_at"):
        return round(float(_CACHE["loaded_at"]), 3)
    return id(df)


def _ask_cache_key(*, mode: str, query: str, df: pd.DataFrame) -> tuple[str, str, Any]:
    return (str(mode or "manual"), str(query or "").strip().lower(), _df_cache_token(df))


def _is_cacheable_auto_payload(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return False
    if meta.get("intent_id") != "auto_mode":
        return False
    auto_meta = meta.get("auto_mode")
    if not isinstance(auto_meta, dict):
        return False
    executed = auto_meta.get("executed_intents")
    if not isinstance(executed, list) or not executed:
        return False
    for item in executed:
        if not isinstance(item, dict):
            return False
        if str(item.get("status") or "").strip().lower() != "ok":
            return False
    return True


def _get_cached_ask_payload(*, mode: str, query: str, df: pd.DataFrame) -> Dict[str, Any] | None:
    if not _ask_cache_enabled() or mode != "auto":
        return None
    key = _ask_cache_key(mode=mode, query=query, df=df)
    now = time.monotonic()
    ttl = float(_ask_cache_ttl_sec())
    with _ASK_CACHE_LOCK:
        expired = [
            cache_key
            for cache_key, item in _ASK_CACHE.items()
            if (now - float(item.get("stored_at", 0.0))) >= ttl
        ]
        for cache_key in expired:
            _ASK_CACHE.pop(cache_key, None)
        cached = _ASK_CACHE.get(key)
        if not isinstance(cached, dict):
            return None
        payload = cached.get("payload")
        if not isinstance(payload, dict) or not _is_cacheable_auto_payload(payload):
            _ASK_CACHE.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _store_cached_ask_payload(*, mode: str, query: str, df: pd.DataFrame, payload: Dict[str, Any]) -> None:
    if not _ask_cache_enabled() or mode != "auto":
        return
    if not _is_cacheable_auto_payload(payload):
        return
    key = _ask_cache_key(mode=mode, query=query, df=df)
    with _ASK_CACHE_LOCK:
        _ASK_CACHE[key] = {
            "stored_at": time.monotonic(),
            "payload": copy.deepcopy(payload),
        }


def _get_rag_rebuild_interval_sec() -> int:
    raw = os.environ.get("ACTUS_RAG_REBUILD_SEC")
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            return 0
    # Disabled by default; opt in explicitly with ACTUS_RAG_REBUILD_SEC.
    return 0


def _rag_rebuild_loop(interval_sec: int) -> None:
    if interval_sec <= 0:
        return
    # Preload already warms the service. Wait a full interval before rebuilding.
    _RAG_REBUILD_STOP.wait(timeout=interval_sec)
    while not _RAG_REBUILD_STOP.is_set():
        try:
            with _RAG_WARMUP_LOCK:
                if _RAG_REBUILD_STOP.is_set():
                    break
                print("[rag] rebuild started")
                build_rag_index.main([])
                print("[rag] rebuild finished")
        except Exception as exc:
            print(f"[rag] rebuild failed: {exc}")
        _RAG_REBUILD_STOP.wait(timeout=interval_sec)


def _start_rag_rebuild_loop() -> None:
    interval_sec = _get_rag_rebuild_interval_sec()
    if interval_sec <= 0:
        return
    thread = threading.Thread(
        target=_rag_rebuild_loop,
        args=(interval_sec,),
        name="rag-rebuild-loop",
        daemon=True,
    )
    thread.start()


def _should_preload_new_rag() -> bool:
    raw = os.environ.get("ACTUS_PRELOAD_NEW_RAG", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _preload_new_rag_service() -> None:
    if not _should_preload_new_rag():
        return
    global _RAG_PRELOAD_STARTED
    with _RAG_PRELOAD_LOCK:
        if _RAG_PRELOAD_STARTED:
            return
        _RAG_PRELOAD_STARTED = True

    def _run() -> None:
        try:
            with _RAG_WARMUP_LOCK:
                print("[rag:new_design] preload started")
                service = get_runtime_service(refresh=False)
                service.warm()
                print(f"[rag:new_design] preload complete (chunks={service.chunk_count})")
        except Exception as exc:
            # Warmup should not block API boot.
            print(f"[rag:new_design] preload failed: {exc}")

    thread = threading.Thread(
        target=_run,
        name="rag-new-design-preload",
        daemon=True,
    )
    thread.start()


@APP.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@APP.get("/api/health/openrouter", dependencies=[Depends(require_internal_request)])
def health_openrouter() -> Dict[str, str]:
    try:
        _openrouter_call("ping")
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_client_safe_detail(
                str(exc),
                generic="Provider health check failed.",
            ),
        ) from exc
    return {"status": "ok"}


@APP.get("/api/user-context", dependencies=[Depends(require_authenticated_request)])
def user_context(email: str | None = None) -> Dict[str, Any]:
    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    try:
        _ensure_firebase_app()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_client_safe_detail(
                str(exc),
                generic="User context backend is unavailable.",
            ),
        ) from exc

    try:
        ref = db.reference("user_roles")
        snapshot = ref.order_by_child("email").equal_to(email).get() or {}
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=_client_safe_detail(
                f"RTDB query failed: {exc}",
                generic="User context lookup failed.",
            ),
        ) from exc
    if not snapshot:
        return {"email": email}

    record = next(iter(snapshot.values()))
    env_profile = record.get("env") if isinstance(record.get("env"), dict) else {}
    first_name = record.get("firstName") or record.get("first_name") or env_profile.get("firstName") or env_profile.get("first_name")
    last_name = record.get("lastName") or record.get("last_name") or env_profile.get("lastName") or env_profile.get("last_name")
    full_name = record.get("name") or record.get("fullName") or env_profile.get("name") or env_profile.get("fullName")
    if not full_name and (first_name or last_name):
        full_name = " ".join([part for part in [first_name, last_name] if part])
    return {
        "email": email,
        "name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "firstName": first_name,
        "lastName": last_name,
        "role": record.get("role"),
        "location": record.get("office") or record.get("location"),
        "last_login": record.get("lastLogin") or record.get("updatedAt"),
    }


@APP.on_event("startup")
def _startup() -> None:
    try:
        init_quality_db()
        print(f"[quality] db ready at {resolve_db_path()}")
    except Exception as exc:
        print(f"[quality] db init failed: {exc}")
    _preload_new_rag_service()
    _start_rag_rebuild_loop()


@APP.on_event("shutdown")
def _shutdown() -> None:
    _RAG_REBUILD_STOP.set()


@APP.post("/api/ask", dependencies=[Depends(require_authenticated_request)])
def ask(payload: AskRequest) -> Dict[str, Any]:
    t0 = time.perf_counter()
    query = payload.query.strip()
    mode = payload.mode
    if not query:
        raise HTTPException(status_code=400, detail="Query is required.")

    llm_mode = os.environ.get("ACTUS_OPENROUTER_MODE", "").strip().lower()
    if mode != "auto" and llm_mode == "always":
        try:
            text = _openrouter_call(query)
            return _ask_response(
                query=query,
                text=text,
                rows=[],
                meta={"provider": "openrouter"},
                t0=t0,
                provider="openrouter",
            )
        except Exception as exc:
            print(f"[openrouter] failed, falling back to actus: {exc}")

    t1 = time.perf_counter()
    try:
        df = _get_df()
    except Exception as exc:
        return _ask_response(
            query=query,
            text=(
                "Actus backend is not configured with Firebase credentials yet. "
                "Set ACTUS_FIREBASE_JSON or ACTUS_FIREBASE_PATH."
            ),
            rows=[],
            meta={},
            t0=t0,
            ok=False,
            provider="actus",
            error=str(exc),
        )
    cached_payload = _get_cached_ask_payload(mode=mode, query=query, df=df)
    if isinstance(cached_payload, dict):
        return _ask_response(
            query=query,
            text=str(cached_payload.get("text") or ""),
            rows=cached_payload.get("rows") if isinstance(cached_payload.get("rows"), list) else [],
            meta=cached_payload.get("meta") if isinstance(cached_payload.get("meta"), dict) else {},
            t0=t0,
            provider="actus_cache",
        )
    if mode == "auto":
        text, df_result, meta = auto_mode_answer(query, df)
    else:
        text, df_result, meta = actus_answer(query, df)
    t2 = time.perf_counter()
    rows: list[dict[str, Any]] = []
    if df_result is not None:
        safe_df = df_result.copy()
        for col in safe_df.columns:
            safe_df[col] = safe_df[col].apply(
                lambda v: v.isoformat() if isinstance(v, pd.Timestamp) else v
            )
        safe_df = safe_df.astype(object).where(pd.notnull(safe_df), None)
        rows = safe_df.to_dict(orient="records")
    if isinstance(meta, dict) and isinstance(meta.get("csv_rows"), pd.DataFrame):
        csv_df = meta["csv_rows"].copy()
        for col in csv_df.columns:
            csv_df[col] = csv_df[col].apply(
                lambda v: v.isoformat() if isinstance(v, pd.Timestamp) else v
            )
        csv_df = csv_df.astype(object).where(pd.notnull(csv_df), None)
        meta = {**meta, "csv_rows": csv_df.to_dict(orient="records")}

    if (
        mode != "auto"
        and llm_mode == "fallback"
        and text.startswith("Right now I can help you with:")
        and not meta.get("is_help")
    ):
        try:
            llm_text = _openrouter_call(query)
            return _ask_response(
                query=query,
                text=llm_text,
                rows=[],
                meta={"provider": "openrouter"},
                t0=t0,
                provider="openrouter",
            )
        except Exception as exc:
            print(f"[openrouter] fallback failed: {exc}")

    t3 = time.perf_counter()
    print(
        "[ask] total={:.1f}ms df={:.1f}ms answer={:.1f}ms serialize={:.1f}ms".format(
            (t3 - t0) * 1000,
            (t1 - t0) * 1000,
            (t2 - t1) * 1000,
            (t3 - t2) * 1000,
        )
    )
    response_payload = _ask_response(
        query=query,
        text=text,
        rows=rows,
        meta=meta,
        t0=t0,
        provider=str(meta.get("provider", "actus")) if isinstance(meta, dict) else "actus",
    )
    _store_cached_ask_payload(mode=mode, query=query, df=df, payload=response_payload)
    return response_payload
