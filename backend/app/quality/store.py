from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


_RETENTION_LOCK = threading.Lock()
_LAST_PRUNE_BY_PATH: dict[str, float] = {}


def _default_db_path() -> Path:
    root = Path(__file__).resolve().parents[2]
    return root / "rag_data" / "quality_metrics.sqlite"


def resolve_db_path(db_path: Optional[str] = None) -> Path:
    raw = db_path or os.environ.get("ACTUS_QUALITY_DB_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _default_db_path()


def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _is_production() -> bool:
    raw = os.environ.get("ACTUS_ENV", "").strip().lower()
    return raw in {"production", "prod"}


def _quality_retention_days() -> int | None:
    raw = os.environ.get("ACTUS_QUALITY_RETENTION_DAYS", "").strip()
    if raw:
        try:
            value = int(raw)
            return value if value > 0 else None
        except ValueError:
            return 90 if _is_production() else None
    return 90 if _is_production() else None


def _prune_interval_sec() -> int:
    raw = os.environ.get("ACTUS_QUALITY_PRUNE_INTERVAL_SEC", "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            return 3600
    return 3600


def prune_quality_events(
    *,
    now_utc: Optional[datetime] = None,
    db_path: Optional[str] = None,
) -> int:
    retention_days = _quality_retention_days()
    if retention_days is None:
        return 0

    now = now_utc or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    cutoff = (now - timedelta(days=retention_days)).isoformat()
    with _connect(db_path) as conn:
        cur = conn.execute(
            """
            DELETE FROM quality_events
            WHERE ts_utc < ?
            """,
            (cutoff,),
        )
        return int(cur.rowcount or 0)


def _maybe_prune_quality_events(db_path: Optional[str] = None) -> None:
    retention_days = _quality_retention_days()
    if retention_days is None:
        return

    interval_sec = _prune_interval_sec()
    db_key = str(resolve_db_path(db_path))
    now_mono = time.monotonic()
    with _RETENTION_LOCK:
        last = _LAST_PRUNE_BY_PATH.get(db_key, 0.0)
        if interval_sec > 0 and (now_mono - last) < interval_sec:
            return
        _LAST_PRUNE_BY_PATH[db_key] = now_mono
    prune_quality_events(db_path=db_path)


def init_quality_db(db_path: Optional[str] = None) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                query TEXT NOT NULL,
                intent_id TEXT,
                provider TEXT,
                latency_ms REAL NOT NULL DEFAULT 0,
                ok INTEGER NOT NULL DEFAULT 1,
                error TEXT,
                result_count INTEGER NOT NULL DEFAULT 0,
                has_ticket_analysis INTEGER NOT NULL DEFAULT 0,
                has_item_analysis INTEGER NOT NULL DEFAULT 0,
                highlight_source TEXT,
                highlight_model TEXT,
                is_help INTEGER NOT NULL DEFAULT 0,
                release_tag TEXT,
                meta_json TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quality_events_ts ON quality_events(ts_utc)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quality_events_intent ON quality_events(intent_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quality_events_release ON quality_events(release_tag)"
        )
    _maybe_prune_quality_events(db_path)


def record_quality_event(event: Dict[str, Any], db_path: Optional[str] = None) -> None:
    init_quality_db(db_path)
    ts_utc = event.get("ts_utc")
    if not ts_utc:
        ts_utc = datetime.now(UTC).isoformat()
    meta_json = event.get("meta_json")
    if meta_json is None and "meta" in event:
        try:
            meta_json = json.dumps(event["meta"], ensure_ascii=True)
        except Exception:
            meta_json = None

    row = (
        str(ts_utc),
        str(event.get("query", "")),
        event.get("intent_id"),
        event.get("provider"),
        float(event.get("latency_ms", 0.0) or 0.0),
        1 if bool(event.get("ok", True)) else 0,
        event.get("error"),
        int(event.get("result_count", 0) or 0),
        1 if bool(event.get("has_ticket_analysis", False)) else 0,
        1 if bool(event.get("has_item_analysis", False)) else 0,
        event.get("highlight_source"),
        event.get("highlight_model"),
        1 if bool(event.get("is_help", False)) else 0,
        event.get("release_tag"),
        meta_json,
    )
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO quality_events (
                ts_utc, query, intent_id, provider, latency_ms, ok, error, result_count,
                has_ticket_analysis, has_item_analysis, highlight_source, highlight_model,
                is_help, release_tag, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )


def _parse_window(window: str, *, default_unit: str = "d") -> int:
    raw = (window or "").strip().lower()
    if not raw:
        return 28
    unit = raw[-1] if raw[-1].isalpha() else default_unit
    number_text = raw[:-1] if raw[-1].isalpha() else raw
    try:
        value = max(1, int(number_text))
    except ValueError:
        return 28
    if unit == "w":
        return value * 7
    if unit == "d":
        return value
    return 28


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    sorted_values = sorted(float(v) for v in values)
    index = int(round(0.95 * (len(sorted_values) - 1)))
    return float(sorted_values[index])


def _metric_bundle(events: Iterable[sqlite3.Row]) -> Dict[str, float]:
    rows = list(events)
    total = len(rows)
    if total == 0:
        return {
            "ask_requests": 0,
            "intent_hit_rate": 0.0,
            "ticket_analysis_rate": 0.0,
            "item_analysis_rate": 0.0,
            "openrouter_highlight_rate": 0.0,
            "avg_result_count": 0.0,
            "p95_latency_ms": 0.0,
            "error_rate": 0.0,
        }

    ok_count = sum(1 for row in rows if int(row["ok"]) == 1)
    intent_hits = sum(1 for row in rows if (row["intent_id"] or "").strip())
    ticket_hits = sum(1 for row in rows if int(row["has_ticket_analysis"]) == 1)
    item_hits = sum(1 for row in rows if int(row["has_item_analysis"]) == 1)
    openrouter_highlights = sum(
        1
        for row in rows
        if str(row["highlight_source"] or "").strip().lower().startswith("openrouter")
    )
    avg_results = sum(float(row["result_count"] or 0) for row in rows) / total
    latencies = [float(row["latency_ms"] or 0.0) for row in rows]

    return {
        "ask_requests": float(total),
        "intent_hit_rate": intent_hits / total,
        "ticket_analysis_rate": ticket_hits / total,
        "item_analysis_rate": item_hits / total,
        "openrouter_highlight_rate": openrouter_highlights / total,
        "avg_result_count": avg_results,
        "p95_latency_ms": _p95(latencies),
        "error_rate": 1.0 - (ok_count / total),
    }


def _round_metrics(metrics: Dict[str, float]) -> Dict[str, float]:
    rounded: Dict[str, float] = {}
    for key, value in metrics.items():
        if key == "ask_requests":
            rounded[key] = int(value)
        else:
            rounded[key] = round(float(value), 4)
    return rounded


def _fetch_rows(start_utc: datetime, end_utc: datetime, db_path: Optional[str]) -> list[sqlite3.Row]:
    init_quality_db(db_path)
    with _connect(db_path) as conn:
        cur = conn.execute(
            """
            SELECT *
            FROM quality_events
            WHERE ts_utc >= ? AND ts_utc < ?
            ORDER BY ts_utc ASC
            """,
            (start_utc.isoformat(), end_utc.isoformat()),
        )
        return cur.fetchall()


def quality_summary(
    *,
    window: str = "28d",
    now_utc: Optional[datetime] = None,
    release_tag: Optional[str] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    now = now_utc or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    days = _parse_window(window)
    current_start = now - timedelta(days=days)
    previous_start = current_start - timedelta(days=days)

    current_rows = _fetch_rows(current_start, now, db_path)
    previous_rows = _fetch_rows(previous_start, current_start, db_path)
    if release_tag:
        current_rows = [row for row in current_rows if (row["release_tag"] or "") == release_tag]
        previous_rows = [row for row in previous_rows if (row["release_tag"] or "") == release_tag]

    current_metrics = _metric_bundle(current_rows)
    previous_metrics = _metric_bundle(previous_rows)
    deltas: Dict[str, float] = {}
    for key, cur_value in current_metrics.items():
        prev_value = previous_metrics.get(key, 0.0)
        deltas[key] = round(float(cur_value) - float(prev_value), 4)

    return {
        "window": f"{days}d",
        "release_tag": release_tag or "all",
        "range": {"start": current_start.isoformat(), "end": now.isoformat()},
        "metrics": _round_metrics(current_metrics),
        "delta_vs_prev_window": _round_metrics(deltas),
    }


def _week_bucket_start(ts_text: str) -> str:
    dt = datetime.fromisoformat(str(ts_text))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    monday = dt.date() - timedelta(days=dt.weekday())
    return monday.isoformat()


def quality_trends(
    *,
    window: str = "12w",
    group_by: str = "week",
    now_utc: Optional[datetime] = None,
    release_tag: Optional[str] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    now = now_utc or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)

    days = _parse_window(window, default_unit="w")
    start = now - timedelta(days=days)
    rows = _fetch_rows(start, now, db_path)
    if release_tag:
        rows = [row for row in rows if (row["release_tag"] or "") == release_tag]

    buckets: Dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        if group_by == "week":
            bucket_key = _week_bucket_start(row["ts_utc"])
        else:
            dt = datetime.fromisoformat(str(row["ts_utc"]))
            bucket_key = dt.date().isoformat()
        buckets.setdefault(bucket_key, []).append(row)

    points = []
    for bucket in sorted(buckets.keys()):
        metrics = _round_metrics(_metric_bundle(buckets[bucket]))
        points.append({"bucket_start": bucket, "metrics": metrics})

    return {
        "window": window,
        "group_by": group_by,
        "release_tag": release_tag or "all",
        "range": {"start": start.isoformat(), "end": now.isoformat()},
        "points": points,
    }
