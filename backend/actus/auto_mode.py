from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import pandas as pd

from actus.intent_router import (
    INTENT_DEFS,
    _fallback_model_name,
    _normalize_query,
    _return_with_intent,
    actus_answer,
)
from actus.intents.credit_ops_snapshot import _parse_window
from actus.intents.customer_analysis import _extract_explicit_customer_query, _extract_match_mode
from actus.intents.item_analysis import _extract_item_number
from actus.intents.ticket_analysis import _extract_ticket_id
from actus.openrouter_client import openrouter_chat

LOGGER = logging.getLogger(__name__)

SPECIALIST_CACHE_TTL_SEC = 120
_SPECIALIST_RESULT_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
_SPECIALIST_RESULT_CACHE_LOCK = threading.Lock()
_CROSS_REQUEST_CACHEABLE_INTENTS = {
    "system_updates",
    "overall_summary",
    "credit_root_causes",
    "credit_trends",
    "credit_anomalies",
    "credit_aging",
    "credit_ops_snapshot",
    "billing_queue_hotspots",
    "root_cause_rtn_timing",
    "top_accounts",
    "top_items",
}


AUTO_FAMILY_ENTITY = "entity"
AUTO_FAMILY_PORTFOLIO = "portfolio"

AUTO_ENTITY_INTENT_IDS = {
    "ticket_analysis",
    "item_analysis",
    "customer_analysis",
    "investigation_notes",
}

AUTO_PORTFOLIO_PRIORITY = (
    "credit_ops_snapshot",
    "system_updates",
    "billing_queue_hotspots",
    "root_cause_rtn_timing",
    "top_accounts",
    "top_items",
    "credit_anomalies",
    "credit_root_causes",
    "credit_trends",
    "credit_aging",
    "overall_summary",
)

AUTO_PORTFOLIO_KEYWORDS: dict[str, tuple[str, ...]] = {
    "credit_ops_snapshot": (
        "ops snapshot",
        "operations snapshot",
        "credit ops",
        "ops review",
        "operations review",
        "throughput",
        "workload",
        "backlog",
    ),
    "system_updates": (
        "system update",
        "system updates",
        "updated by the system",
        "system rtn",
        "rtn updates",
        "batch update",
        "batch updates",
    ),
    "billing_queue_hotspots": (
        "billing queue",
        "queue delay",
        "queue delays",
    ),
    "root_cause_rtn_timing": (
        "root causes taking the longest",
        "root cause taking the longest",
        "longest rtn assignment",
        "time to rtn",
        "days to rtn",
        "reach rtn assignment",
    ),
    "top_accounts": (
        "top customers",
        "top accounts",
        "customers driving",
        "accounts driving",
    ),
    "top_items": (
        "top items",
        "items driving",
        "products driving",
    ),
    "credit_anomalies": (
        "anomal",
        "outlier",
        "unusual",
        "suspicious",
        "hard cap",
    ),
    "credit_root_causes": (
        "root cause",
        "root causes",
        "reason for credit",
        "reasons for credit",
        "credit cause",
        "credit causes",
    ),
    "credit_trends": (
        "trend",
        "trends",
        "pattern",
        "patterns",
        "insight",
        "insights",
        "happening",
    ),
    "credit_aging": (
        "aging",
        "ageing",
        "older than",
        "days open",
    ),
    "overall_summary": (
        "overview",
        "summary",
        "summarize",
        "picture",
        "status",
        "health",
    ),
}

AUTO_NOTES_KEYWORDS = (
    "investigation",
    "notes",
    "note",
    "evidence",
    "documentation",
    "document",
    "details",
    "detail",
    "supporting",
    "proof",
)

DEFAULT_AUTO_SUMMARY_PRIMARY_MODEL = "openai/gpt-4o-mini"

_ACRONYM_PARTS = {
    "ppd": "PPD",
    "rtn": "RTN",
    "cr": "CR",
    "wip": "WIP",
}


@dataclass(frozen=True)
class PlannedIntent:
    id: str
    label: str
    query: str


@dataclass(frozen=True)
class AutoPlan:
    family: str
    primary_intent: str
    target_label: str | None
    intents: tuple[PlannedIntent, ...]
    suggestions: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class SpecialistRun:
    plan: PlannedIntent
    text: str
    rows: pd.DataFrame | None
    meta: dict[str, Any]


def _intent_def_by_id(intent_id: str) -> dict[str, Any] | None:
    for item in INTENT_DEFS:
        if item["id"] == intent_id:
            return item
    return None


def _plan_intent(intent_id: str, query: str) -> PlannedIntent:
    intent_def = _intent_def_by_id(intent_id)
    if intent_def is None:
        raise KeyError(f"Unknown auto intent: {intent_id}")
    return PlannedIntent(
        id=intent_id,
        label=str(intent_def["label"]),
        query=query,
    )


def _is_classifier_enabled() -> bool:
    raw = os.environ.get("ACTUS_INTENT_CLASSIFIER", "").strip().lower()
    return raw in {"1", "true", "yes"}


def _query_mentions_notes(normalized: str) -> bool:
    return any(term in normalized for term in AUTO_NOTES_KEYWORDS)


def _normalize_customer_target(query: str) -> tuple[str | None, str]:
    customer_query = _extract_explicit_customer_query(query)
    if not customer_query:
        return None, "account_prefix"
    return customer_query, _extract_match_mode(query)


def _mentions_any(normalized: str, terms: tuple[str, ...]) -> bool:
    return any(term in normalized for term in terms)


def _is_root_cause_rtn_timing_query(normalized: str) -> bool:
    has_root_cause = "root cause" in normalized or "root causes" in normalized
    has_timing = _mentions_any(
        normalized,
        (
            "longest",
            "slowest",
            "taking the longest",
            "rtn assignment",
            "days to rtn",
            "time to rtn",
            "reach rtn",
        ),
    )
    return has_root_cause and has_timing


def _is_top_accounts_query(normalized: str) -> bool:
    has_customer = _mentions_any(normalized, ("account", "accounts", "customer", "customers"))
    has_rank = _mentions_any(normalized, ("most", "top", "highest", "biggest", "driving", "leading"))
    has_scope = _mentions_any(normalized, ("credit", "credits", "credited", "volume", "exposure", "liability"))
    return has_customer and has_rank and has_scope


def _is_top_items_query(normalized: str) -> bool:
    has_item = _mentions_any(normalized, ("item", "items", "sku", "skus", "product", "products"))
    has_rank = _mentions_any(normalized, ("most", "top", "highest", "biggest", "driving", "leading"))
    has_scope = _mentions_any(normalized, ("credit", "credits", "credited", "volume", "exposure", "liability"))
    return has_item and has_rank and has_scope


def _is_billing_queue_query(normalized: str) -> bool:
    return "billing queue" in normalized and _mentions_any(
        normalized,
        ("delay", "delays", "accumulating", "where", "stuck", "backlog"),
    )


def _is_explicit_customer_entity_query(query: str, normalized: str) -> bool:
    if _extract_explicit_customer_query(query):
        return True
    return normalized.startswith("analyze account") or normalized.startswith("analyze customer")


def _portfolio_intent_matches(intent_id: str, normalized: str) -> bool:
    if intent_id == "root_cause_rtn_timing":
        return _is_root_cause_rtn_timing_query(normalized)
    if intent_id == "billing_queue_hotspots":
        return _is_billing_queue_query(normalized)
    if intent_id == "top_accounts":
        return _is_top_accounts_query(normalized)
    if intent_id == "top_items":
        return _is_top_items_query(normalized)
    if intent_id == "credit_root_causes" and _is_root_cause_rtn_timing_query(normalized):
        return False
    keywords = AUTO_PORTFOLIO_KEYWORDS.get(intent_id, ())
    return any(keyword in normalized for keyword in keywords)


def _query_has_credit_scope(normalized: str) -> bool:
    return any(term in normalized for term in ("credit", "credits", "ticket", "tickets", "invoice", "invoices"))


def _query_mentions_overview(normalized: str) -> bool:
    return any(term in normalized for term in ("overview", "summary", "picture", "health", "status"))


def _looks_like_portfolio_query(normalized: str) -> bool:
    if not _query_has_credit_scope(normalized):
        return False
    return any(_portfolio_intent_matches(intent_id, normalized) for intent_id in AUTO_PORTFOLIO_PRIORITY)


def _build_portfolio_query(intent_id: str, query: str) -> str:
    intent_def = _intent_def_by_id(intent_id)
    if intent_def is None:
        return query
    prefix = str(intent_def["prefix"]).strip()
    normalized_query = _normalize_query(query)
    if prefix and prefix in normalized_query:
        return query
    if prefix:
        return f"{prefix} {query}".strip()
    return query


def _portfolio_follow_up_suggestions(query: str) -> list[dict[str, str]]:
    normalized = _normalize_query(query)
    suggestions: list[dict[str, str]] = []
    for intent_id in AUTO_PORTFOLIO_PRIORITY:
        if not _portfolio_intent_matches(intent_id, normalized):
            continue
        intent_def = _intent_def_by_id(intent_id)
        if intent_def is None:
            continue
        suggestions.append(
            {
                "id": intent_id,
                "label": str(intent_def["label"]),
                "prefix": _build_portfolio_query(intent_id, query),
            }
        )
    return suggestions[:3]


def _merge_suggestions(*suggestion_sets: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen: set[str] = set()
    for suggestion_set in suggestion_sets:
        for item in suggestion_set or []:
            prefix = str(item.get("prefix") or "").strip()
            label = str(item.get("label") or "").strip()
            intent_id = str(item.get("id") or "").strip()
            if not prefix or not label:
                continue
            key = prefix.lower()
            if key in seen:
                continue
            seen.add(key)
            payload = {"label": label, "prefix": prefix}
            if intent_id:
                payload["id"] = intent_id
            merged.append(payload)
    return merged[:4]


def _build_entity_plan(query: str, normalized: str) -> AutoPlan | None:
    ticket_id = _extract_ticket_id(query)
    if ticket_id:
        intents = [_plan_intent("ticket_analysis", f"analyze ticket {ticket_id}")]
        if _query_mentions_notes(normalized):
            intents.append(_plan_intent("investigation_notes", f"investigation notes for ticket {ticket_id}"))
        return AutoPlan(
            family=AUTO_FAMILY_ENTITY,
            primary_intent="ticket_analysis",
            target_label=ticket_id,
            intents=tuple(intents),
            suggestions=tuple(_portfolio_follow_up_suggestions(query)),
        )

    if "item" in normalized:
        item_number = _extract_item_number(query)
        if item_number:
            return AutoPlan(
                family=AUTO_FAMILY_ENTITY,
                primary_intent="item_analysis",
                target_label=item_number,
                intents=( _plan_intent("item_analysis", f"analyze item {item_number}"), ),
                suggestions=tuple(_portfolio_follow_up_suggestions(query)),
            )

    if _is_explicit_customer_entity_query(query, normalized):
        customer_query, match_mode = _normalize_customer_target(query)
        if customer_query:
            if match_mode == "customer_number":
                auto_query = f"analyze customer number {customer_query}"
            else:
                auto_query = f"analyze account {customer_query}"
            return AutoPlan(
                family=AUTO_FAMILY_ENTITY,
                primary_intent="customer_analysis",
                target_label=customer_query,
                intents=( _plan_intent("customer_analysis", auto_query), ),
                suggestions=tuple(_portfolio_follow_up_suggestions(query)),
            )

    return None


def _build_portfolio_plan(query: str, normalized: str) -> AutoPlan | None:
    selected: list[str] = []
    for intent_id in AUTO_PORTFOLIO_PRIORITY:
        if _portfolio_intent_matches(intent_id, normalized):
            selected.append(intent_id)
    if not selected and _query_has_credit_scope(normalized):
        if _query_mentions_overview(normalized):
            selected.append("overall_summary")
    if not selected:
        return None

    if "overall_summary" in selected and _query_mentions_overview(normalized):
        selected = ["overall_summary", *[intent_id for intent_id in selected if intent_id != "overall_summary"]]

    selected = selected[:3]
    plans = tuple(_plan_intent(intent_id, _build_portfolio_query(intent_id, query)) for intent_id in selected)
    return AutoPlan(
        family=AUTO_FAMILY_PORTFOLIO,
        primary_intent=selected[0],
        target_label=None,
        intents=plans,
        suggestions=tuple(),
    )


def _plan_with_llm(query: str) -> AutoPlan | None:
    if not _is_classifier_enabled():
        return None

    catalog = []
    for intent_id in (*AUTO_ENTITY_INTENT_IDS, *AUTO_PORTFOLIO_PRIORITY):
        intent_def = _intent_def_by_id(intent_id)
        if intent_def is None:
            continue
        catalog.append(
            {
                "id": intent_id,
                "label": intent_def["label"],
                "family": AUTO_FAMILY_ENTITY if intent_id in AUTO_ENTITY_INTENT_IDS else AUTO_FAMILY_PORTFOLIO,
            }
        )

    normalized = _normalize_query(query)
    system_prompt = (
        "You plan Auto Mode specialist execution for a credit ops assistant. "
        "Return ONLY JSON with keys `family` and `intents`. "
        "Rules: choose either `entity` or `portfolio`; never mix families; "
        "limit to up to 3 intents; for entity mode use ticket/item/customer plus optional investigation_notes."
    )
    user_prompt = json.dumps(
        {
            "query": query,
            "normalized_query": normalized,
            "catalog": catalog,
        }
    )

    def _call(model: str | None = None) -> str:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        if model:
            return openrouter_chat(messages, model=model)
        return openrouter_chat(messages)

    fallback_model = _fallback_model_name()
    raw: str
    try:
        raw = _call()
    except Exception:
        if not fallback_model:
            return None
        try:
            raw = _call(fallback_model)
        except Exception:
            return None

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*", "", cleaned).strip()
        cleaned = cleaned.rstrip("`").strip()

    try:
        payload = json.loads(cleaned)
    except Exception:
        return None

    family = str(payload.get("family") or "").strip().lower()
    intent_ids = payload.get("intents")
    if family not in {AUTO_FAMILY_ENTITY, AUTO_FAMILY_PORTFOLIO} or not isinstance(intent_ids, list):
        return None

    requested_ids = [str(item).strip() for item in intent_ids if str(item).strip()]
    if not requested_ids:
        return None

    entity_plan = _build_entity_plan(query, normalized)
    if family == AUTO_FAMILY_ENTITY:
        if entity_plan is None:
            return None
        allowed = {"ticket_analysis", "item_analysis", "customer_analysis", "investigation_notes"}
        if any(intent_id not in allowed for intent_id in requested_ids[:2]):
            return None
        intents = [entity_plan.intents[0]]
        if "investigation_notes" in requested_ids and entity_plan.primary_intent == "ticket_analysis":
            intents = [
                _plan_intent("ticket_analysis", entity_plan.intents[0].query),
                _plan_intent("investigation_notes", f"investigation notes for ticket {entity_plan.target_label}"),
            ]
        return AutoPlan(
            family=entity_plan.family,
            primary_intent=entity_plan.primary_intent,
            target_label=entity_plan.target_label,
            intents=tuple(intents),
            suggestions=entity_plan.suggestions,
        )

    selected = [intent_id for intent_id in AUTO_PORTFOLIO_PRIORITY if intent_id in requested_ids][:3]
    if not selected:
        return None
    return AutoPlan(
        family=AUTO_FAMILY_PORTFOLIO,
        primary_intent=selected[0],
        target_label=None,
        intents=tuple(_plan_intent(intent_id, _build_portfolio_query(intent_id, query)) for intent_id in selected),
        suggestions=tuple(),
    )


def plan_auto_mode(query: str) -> AutoPlan | None:
    normalized = _normalize_query(query)
    entity_plan = _build_entity_plan(query, normalized)
    if entity_plan is not None:
        return entity_plan

    portfolio_plan = _build_portfolio_plan(query, normalized)
    if portfolio_plan is not None:
        return portfolio_plan

    return _plan_with_llm(query)


def _execute_planned_intent(plan: PlannedIntent, df: pd.DataFrame) -> SpecialistRun:
    cache = df.attrs.setdefault("_actus_intent_cache", {}) if isinstance(getattr(df, "attrs", None), dict) else None
    if isinstance(cache, dict):
        cached_result = cache.get((plan.id, plan.query))
        if isinstance(cached_result, tuple) and len(cached_result) == 3:
            text, rows, meta = cached_result
            return SpecialistRun(plan=plan, text=text, rows=_clone_cached_rows(rows), meta=_clone_cached_meta(meta))

    cross_request_cached = _get_cached_specialist_run(plan=plan, df=df)
    if cross_request_cached is not None:
        if isinstance(cache, dict):
            cache[(plan.id, plan.query)] = (
                cross_request_cached.text,
                cross_request_cached.rows,
                cross_request_cached.meta,
            )
        return cross_request_cached

    intent_def = _intent_def_by_id(plan.id)
    if intent_def is None:
        raise RuntimeError(f"Unknown auto intent: {plan.id}")
    result = intent_def["func"](plan.query, df)
    if result is None:
        raise RuntimeError(f"{plan.id} returned no result")
    text, rows, meta = _return_with_intent(result, intent_id=plan.id, matched_by="auto_mode")
    if isinstance(cache, dict):
        cache[(plan.id, plan.query)] = (text, rows, meta)
    run = SpecialistRun(plan=plan, text=text, rows=rows, meta=meta)
    _store_cached_specialist_run(run=run, df=df)
    return run


def _specialist_cache_enabled() -> bool:
    raw = os.environ.get("ACTUS_SPECIALIST_CACHE_ENABLED", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _specialist_cache_ttl_sec() -> int:
    raw = os.environ.get("ACTUS_SPECIALIST_CACHE_TTL_SEC", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            return SPECIALIST_CACHE_TTL_SEC
    return SPECIALIST_CACHE_TTL_SEC


def _df_cache_token(df: pd.DataFrame) -> Any:
    attrs = getattr(df, "attrs", None)
    if isinstance(attrs, dict) and attrs.get("_actus_df_cache_token") is not None:
        return attrs.get("_actus_df_cache_token")
    return id(df)


def _window_cache_label(query: str) -> str:
    try:
        start, end, raw_label = _parse_window(query)
    except Exception:
        return ""
    start_ts = pd.Timestamp(start).tz_localize(None) if start is not None and not pd.isna(start) and getattr(start, "tzinfo", None) is not None else pd.Timestamp(start) if start is not None and not pd.isna(start) else None
    end_ts = pd.Timestamp(end).tz_localize(None) if end is not None and not pd.isna(end) and getattr(end, "tzinfo", None) is not None else pd.Timestamp(end) if end is not None and not pd.isna(end) else None
    if start_ts is None:
        return str(raw_label or "").strip().lower()
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    return f"{start_ts.date()}→{end_ts.date()}"


def _ranking_scope_key(query: str) -> str:
    normalized = _normalize_query(query)
    if any(term in normalized for term in ("open exposure", "open credit", "open credits", "open liability", "open volume")):
        return "open"
    if any(term in normalized for term in ("credited", "issued", "credit number", "credit numbers", "rtn", "volume")):
        return "credited"
    if "exposure" in normalized or "liability" in normalized:
        return "open"
    return "credited"


def _specialist_cache_signature(plan: PlannedIntent) -> tuple[Any, ...]:
    window = _window_cache_label(plan.query)
    if plan.id in {
        "system_updates",
        "overall_summary",
        "credit_root_causes",
        "credit_trends",
        "credit_anomalies",
        "credit_aging",
        "credit_ops_snapshot",
        "billing_queue_hotspots",
        "root_cause_rtn_timing",
    }:
        return (plan.id, window)
    if plan.id in {"top_accounts", "top_items"}:
        return (plan.id, window, _ranking_scope_key(plan.query))
    return (plan.id, _normalize_query(plan.query))


def _specialist_cache_key(*, plan: PlannedIntent, df: pd.DataFrame) -> tuple[Any, ...]:
    return (_df_cache_token(df),) + _specialist_cache_signature(plan)


def _clone_cached_rows(rows: pd.DataFrame | None) -> pd.DataFrame | None:
    if isinstance(rows, pd.DataFrame):
        return rows.copy(deep=False)
    return rows


def _clone_cached_meta(meta: dict[str, Any]) -> dict[str, Any]:
    return dict(meta) if isinstance(meta, dict) else {}


def _get_cached_specialist_run(*, plan: PlannedIntent, df: pd.DataFrame) -> SpecialistRun | None:
    if not _specialist_cache_enabled() or plan.id not in _CROSS_REQUEST_CACHEABLE_INTENTS:
        return None
    key = _specialist_cache_key(plan=plan, df=df)
    now = time.monotonic()
    ttl = float(_specialist_cache_ttl_sec())
    with _SPECIALIST_RESULT_CACHE_LOCK:
        expired = [
            cache_key
            for cache_key, item in _SPECIALIST_RESULT_CACHE.items()
            if (now - float(item.get("stored_at", 0.0))) >= ttl
        ]
        for cache_key in expired:
            _SPECIALIST_RESULT_CACHE.pop(cache_key, None)
        cached = _SPECIALIST_RESULT_CACHE.get(key)
    if not isinstance(cached, dict):
        return None
    return SpecialistRun(
        plan=plan,
        text=str(cached.get("text") or ""),
        rows=_clone_cached_rows(cached.get("rows")),
        meta=_clone_cached_meta(cached.get("meta") if isinstance(cached.get("meta"), dict) else {}),
    )


def _store_cached_specialist_run(*, run: SpecialistRun, df: pd.DataFrame) -> None:
    if not _specialist_cache_enabled() or run.plan.id not in _CROSS_REQUEST_CACHEABLE_INTENTS:
        return
    key = _specialist_cache_key(plan=run.plan, df=df)
    with _SPECIALIST_RESULT_CACHE_LOCK:
        _SPECIALIST_RESULT_CACHE[key] = {
            "stored_at": time.monotonic(),
            "text": run.text,
            "rows": _clone_cached_rows(run.rows),
            "meta": _clone_cached_meta(run.meta),
        }


def _format_money(value: Any) -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return "N/A"
    return f"${amount:,.2f}"


def _format_count(value: Any) -> str:
    try:
        count = int(float(value))
    except (TypeError, ValueError):
        return "0"
    return f"{count:,}"


def _format_calendar_date(value: Any) -> str:
    try:
        ts = pd.to_datetime(value, errors="raise")
    except Exception:
        return str(value or "").strip()
    if pd.isna(ts):
        return str(value or "").strip()
    return f"{ts.strftime('%B')} {int(ts.day)}, {int(ts.year)}"


def _humanize_window_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text or "→" not in text:
        return text
    start_text, end_text = [part.strip() for part in text.split("→", 1)]
    return f"{_format_calendar_date(start_text)} – {_format_calendar_date(end_text)}"


def _humanize_identifier(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "Unspecified"
    if " " in text:
        return text
    parts = re.split(r"[_\-\s]+", text)
    humanized: list[str] = []
    for part in parts:
        lowered = part.lower()
        if lowered in _ACRONYM_PARTS:
            humanized.append(_ACRONYM_PARTS[lowered])
        else:
            humanized.append(part.capitalize())
    return " ".join(humanized)


def _strip_markdown(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"__([^_]+)__", r"\1", text)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", text)
    return " ".join(text.split())


def _generic_text_bullets(text: str, *, max_items: int = 3) -> list[str]:
    lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        cleaned = _strip_markdown(raw_line).lstrip("-•").strip()
        if not cleaned:
            continue
        lines.append(cleaned)
        if len(lines) >= max_items:
            break
    return lines


def _clean_llm_json(raw: str) -> str:
    cleaned = str(raw or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    return cleaned


def _resolve_auto_summary_models() -> tuple[str | None, str | None]:
    primary = (
        os.environ.get("ACTUS_OPENROUTER_AUTO_MODE_MODEL")
        or os.environ.get("ACTUS_OPENROUTER_SUMMARY_MODEL")
        or os.environ.get("ACTUS_OPENROUTER_MODEL")
    )
    fallback = (
        os.environ.get("ACTUS_OPENROUTER_AUTO_MODE_MODEL_FALLBACK")
        or os.environ.get("ACTUS_OPENROUTER_SUMMARY_MODEL_FALLBACK")
        or os.environ.get("ACTUS_OPENROUTER_MODEL_FALLBACK")
        or _fallback_model_name()
    )
    if primary and fallback and primary == fallback:
        fallback = None
    return primary, fallback


def _resolve_auto_summary_primary_model_name(model_override: str | None) -> str:
    if model_override:
        return model_override
    return (
        os.environ.get("ACTUS_OPENROUTER_AUTO_MODE_MODEL", "").strip()
        or os.environ.get("ACTUS_OPENROUTER_SUMMARY_MODEL", "").strip()
        or os.environ.get("ACTUS_OPENROUTER_MODEL", "").strip()
        or DEFAULT_AUTO_SUMMARY_PRIMARY_MODEL
    )


def _ticket_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("ticket_analysis")
    if not isinstance(payload, dict):
        return []
    answer_text = str(payload.get("answer") or "").strip()
    if answer_text.endswith("was not found."):
        ticket_id = payload.get("ticket_id") or "N/A"
        return [
            f"Ticket **{ticket_id}** was not found in the current canonical snapshot.",
            "Run a live refresh if the ticket was added recently, then retry the analysis.",
        ]
    bullets: list[str] = []
    primary_root = _humanize_identifier(payload.get("primary_root_cause"))
    support = [
        _humanize_identifier(value)
        for value in (payload.get("supporting_root_causes") or [])
        if str(value or "").strip()
    ]
    support_suffix = f"; supporting causes: {', '.join(support[:3])}" if support else ""
    bullets.append(
        f"Ticket **{payload.get('ticket_id') or 'N/A'}** has primary root cause **{primary_root}**{support_suffix}."
    )
    bullets.append(
        f"Exposure is **{_format_money(payload.get('credit_total'))}** across **{int(payload.get('line_count') or 0)}** invoice line(s)."
    )
    if payload.get("is_partially_credited"):
        bullets.append(
            f"Credit coverage is partial: **{int(payload.get('credited_line_count') or 0)}** credited line(s) and **{int(payload.get('pending_line_count') or 0)}** pending line(s)."
        )
    elif payload.get("is_credited"):
        bullets.append("The ticket is fully credited.")
    else:
        days_pending = payload.get("days_pending_billing_to_credit")
        if isinstance(days_pending, (int, float)):
            bullets.append(f"The ticket is still pending in billing after **{days_pending:.2f}** day(s).")
        else:
            bullets.append("The ticket is still open.")
    highlights = payload.get("investigation_highlights") or []
    if highlights:
        bullets.append(f"Evidence highlight: {_strip_markdown(highlights[0])}")
    return bullets[:4]


def _item_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("item_analysis")
    if not isinstance(payload, dict):
        return []
    bullets = [
        f"Item **{payload.get('item_number') or 'N/A'}** appears in **{int(payload.get('ticket_count') or 0)}** ticket(s), **{int(payload.get('invoice_count') or 0)}** invoice(s), and **{int(payload.get('line_count') or 0)}** matched line(s).",
        f"Total credit exposure is **{_format_money(payload.get('total_credit'))}**.",
    ]
    root_counts = payload.get("root_cause_counts")
    if isinstance(root_counts, dict) and root_counts:
        top_root = max(root_counts.items(), key=lambda item: item[1])
        bullets.append(
            f"Most common root cause is **{_humanize_identifier(top_root[0])}** with **{int(top_root[1])}** matched line(s)."
        )
    first_seen = payload.get("first_seen")
    last_seen = payload.get("last_seen")
    if first_seen or last_seen:
        bullets.append(f"Observed activity ranges from **{first_seen or 'unknown'}** to **{last_seen or 'unknown'}**.")
    return bullets[:4]


def _customer_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("customer_analysis")
    if not isinstance(payload, dict):
        return []
    label = "Account prefix" if payload.get("match_mode") != "customer_number" else "Customer number"
    bullets = [
        f"{label} **{payload.get('normalized_query') or payload.get('query') or 'N/A'}** matches **{int(payload.get('ticket_count') or 0)}** ticket(s), **{int(payload.get('invoice_count') or 0)}** invoice(s), and **{int(payload.get('item_count') or 0)}** item(s).",
        f"Total exposure is **{_format_money(payload.get('credit_total'))}** across **{int(payload.get('line_count') or 0)}** line(s).",
        (
            "Ticket mix: "
            f"**{int(payload.get('fully_credited_ticket_count') or 0)}** fully credited, "
            f"**{int(payload.get('partially_credited_ticket_count') or 0)}** partially credited, "
            f"**{int(payload.get('open_ticket_count') or 0)}** open."
        ),
    ]
    root_counts = payload.get("root_cause_counts_primary")
    if isinstance(root_counts, dict) and root_counts:
        top_root = max(root_counts.items(), key=lambda item: item[1])
        bullets.append(
            f"Most common primary root cause is **{_humanize_identifier(top_root[0])}** across **{int(top_root[1])}** ticket(s)."
        )
    return bullets[:4]


def _investigation_note_bullets(meta: dict[str, Any], text: str) -> list[str]:
    summary = meta.get("note_summary")
    if isinstance(summary, dict):
        bullets = [_strip_markdown(item) for item in (summary.get("bullets") or []) if _strip_markdown(item)]
        if bullets:
            return bullets[:3]

    parsed_items: list[str] = []
    ticket_match = re.search(r"Here are the investigation notes for (.+?)\.", text)
    if ticket_match:
        ticket_label = _strip_markdown(ticket_match.group(1))
        if ticket_label:
            parsed_items.append(f"Investigation notes are available for **{ticket_label}**.")

    for raw_line in str(text or "").splitlines():
        match = re.match(
            r"^\s*-\s+\*\*(?P<combo>[^*]+)\*\*\s+—\s+(?P<title>.*?)(?:\s+\(Updated:\s*(?P<updated>.*?)\))?\s+•\s+Note ID:",
            raw_line,
        )
        if not match:
            continue
        combo = _strip_markdown(match.group("combo"))
        title = _strip_markdown(match.group("title"))
        updated = _strip_markdown(match.group("updated"))
        bullet = f"Evidence note available for combo **{combo or 'N/A'}**"
        if title and title != "N/A":
            bullet += f": {title}"
        if updated and updated != "N/A":
            bullet += f" (updated {updated})"
        parsed_items.append(bullet + ".")
        if len(parsed_items) >= 3:
            break

    if parsed_items:
        return parsed_items[:3]
    return _generic_text_bullets(text, max_items=3)


def _anomaly_bullets(meta: dict[str, Any], rows: pd.DataFrame | None) -> list[str]:
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame()
    preview_count = int(len(frame.index))
    total_count = int(meta.get("csv_row_count") or preview_count)
    payload = meta.get("creditAnomalies")
    if isinstance(payload, dict) and payload.get("window"):
        bullets = [
            f"In **{payload.get('window')}**, anomaly scan flagged **{total_count}** row(s) for review; the preview includes **{preview_count}** row(s).",
        ]
    else:
        bullets = [
            f"Anomaly scan flagged **{total_count}** row(s) for review; the preview includes **{preview_count}** row(s).",
        ]
    if not frame.empty:
        amount_col = "Credit Request Total" if "Credit Request Total" in frame.columns else None
        if amount_col:
            numeric = pd.to_numeric(frame[amount_col], errors="coerce").dropna()
            if not numeric.empty:
                bullets.append(f"Largest preview anomaly is **{_format_money(float(numeric.max()))}**.")
        flag_col = "Anomaly Flag" if "Anomaly Flag" in frame.columns else None
        if flag_col:
            counts = frame[flag_col].astype(str).str.strip().value_counts()
            if not counts.empty:
                top_flag, top_count = counts.index[0], int(counts.iloc[0])
                bullets.append(f"Most common preview flag is **{top_flag}** on **{top_count}** row(s).")
    return bullets[:3]


def _root_cause_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("rootCauses")
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or []
    if not isinstance(data, list) or not data:
        return ["No root-cause rows were returned."]
    bullets: list[str] = []
    for item in data[:3]:
        if not isinstance(item, dict):
            continue
        bullets.append(
            f"**{item.get('root_cause') or 'Unspecified'}**: {_format_money(item.get('credit_request_total'))} across **{int(item.get('record_count') or 0)}** record(s)."
        )
    if payload.get("total"):
        bullets.insert(0, f"Total exposure across returned root-cause groups is **{payload.get('total')}**.")
    return bullets[:4]


def _top_accounts_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("top_accounts_summary")
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or []
    scope = str(payload.get("scope") or "credited").strip()
    scope_label = "credited volume" if scope == "credited" else "open exposure"
    bullets = [
        f"In **{payload.get('window') or 'the selected window'}**, customer concentration for **{scope_label}** totals **{_format_money(payload.get('total_credit'))}**.",
    ]
    for item in data[:3]:
        if not isinstance(item, dict):
            continue
        bullets.append(
            f"**{item.get('label') or 'N/A'}** drives **{_format_money(item.get('credit_total'))}** across **{int(item.get('record_count') or 0)}** record(s)."
        )
    return bullets[:4]


def _top_items_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("top_items_summary")
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or []
    scope = str(payload.get("scope") or "credited").strip()
    scope_label = "credited volume" if scope == "credited" else "open exposure"
    bullets = [
        f"In **{payload.get('window') or 'the selected window'}**, item concentration for **{scope_label}** totals **{_format_money(payload.get('total_credit'))}**.",
    ]
    for item in data[:3]:
        if not isinstance(item, dict):
            continue
        bullets.append(
            f"**{item.get('label') or 'N/A'}** drives **{_format_money(item.get('credit_total'))}** across **{int(item.get('record_count') or 0)}** record(s)."
        )
    return bullets[:4]


def _trend_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("creditTrends")
    if not isinstance(payload, dict):
        return []
    metrics = payload.get("metrics") or []
    bullets: list[str] = []
    if isinstance(metrics, list):
        for metric in metrics[:3]:
            if not isinstance(metric, dict):
                continue
            current = metric.get("current")
            previous = metric.get("previous")
            change = metric.get("change")
            is_currency = bool(metric.get("isCurrency"))
            current_text = _format_money(current) if is_currency else str(current)
            previous_text = _format_money(previous) if is_currency else str(previous)
            bullets.append(
                f"**{metric.get('label') or 'Metric'}**: {current_text} vs {previous_text} ({float(change or 0):+.1f}%)."
            )
    window = payload.get("window")
    if isinstance(window, dict) and window.get("current") and window.get("previous"):
        bullets.append(
            f"Comparison window: **{window.get('previous')}** against **{window.get('current')}**."
        )
    return bullets[:4]


def _aging_bullets(text: str) -> list[str]:
    bullets: list[str] = []
    open_match = re.search(r"Total open tickets:\s*\*\*(\d[\d,]*)\*\*", text)
    oldest_match = re.search(r"Oldest open:\s*\*\*(\d+(?:\.\d+)?)\*\*\s*days", text)
    exposure_match = re.search(r"Total credit exposure:\s*\*\*([^*]+)\*\*", text)
    if open_match:
        bullets.append(f"Open tickets without RTN/CR: **{open_match.group(1)}**.")
    if oldest_match:
        bullets.append(f"Oldest open exposure is **{oldest_match.group(1)}** day(s).")
    if exposure_match:
        bullets.append(f"Total open credit exposure is **{exposure_match.group(1).strip()}**.")
    return bullets[:3] or _generic_text_bullets(text, max_items=3)


def _ops_snapshot_bullets(text: str) -> list[str]:
    bullets: list[str] = []
    window_match = re.search(r"Window used:\s*\*\*([^*]+)\*\*", text)
    records_match = re.search(r"Records found:\s*\*\*([\d,]+)\*\*", text)
    total_match = re.search(r"Total credited:\s*\*\*([^*]+)\*\*", text)
    cause_match = re.search(r"Primary root cause:\s*\*\*([^*]+)\*\*", text)
    if window_match:
        bullets.append(f"Window used: **{window_match.group(1).strip()}**.")
    if records_match:
        bullets.append(f"Snapshot returned **{records_match.group(1)}** record(s).")
    if total_match:
        bullets.append(f"Total credited in scope is **{total_match.group(1).strip()}**.")
    if cause_match:
        bullets.append(f"Primary root cause in scope is **{cause_match.group(1).strip()}**.")
    return bullets[:4] or _generic_text_bullets(text, max_items=4)


def _overall_summary_bullets(meta: dict[str, Any], text: str) -> list[str]:
    payload = meta.get("overall_summary")
    if isinstance(payload, dict):
        bullets: list[str] = []
        window = str(payload.get("window") or "the selected window").strip()
        credited = payload.get("credited_in_period") if isinstance(payload.get("credited_in_period"), dict) else {}
        bullets.append(
            f"In **{window}**, open exposure is **{_format_money(payload.get('open_credit_total'))}** across **{int(payload.get('open_record_count') or 0)}** record(s)."
        )
        bullets.append(
            f"Average open age is **{float(payload.get('avg_days_open') or 0.0):.1f}** day(s) with **{float(payload.get('avg_days_since_last_status') or 0.0):.1f}** day(s) since the last update; billing queue delay affects **{int(payload.get('billing_queue_delay_count') or 0)}** record(s) totaling **{_format_money(payload.get('billing_queue_delay_total'))}**, and stale investigation affects **{int(payload.get('stale_investigation_count') or 0)}** record(s) totaling **{_format_money(payload.get('stale_investigation_total'))}**."
        )
        bullets.append(
            f"What was credited in period: **{_format_money(credited.get('credited_credit_total'))}** across **{int(credited.get('credited_record_count') or 0)}** unique record(s); average time to RTN assignment is **{float(credited.get('avg_days_to_rtn_assignment') or 0.0):.1f}** day(s)."
        )
        bullets.append(
            f"Primary attribution is **system-led {int(credited.get('primary_system_record_count') or 0)} / {_format_money(credited.get('primary_system_credit_total'))}** and **manual-led {int(credited.get('primary_manual_record_count') or 0)} / {_format_money(credited.get('primary_manual_credit_total'))}**; reopened after terminal totals **{int(credited.get('reopened_after_terminal_count') or 0)}** record(s)."
        )

        return bullets[:4]
    return _generic_text_bullets(text, max_items=4)


def _system_updates_bullets(meta: dict[str, Any], text: str) -> list[str]:
    payload = meta.get("system_updates_summary")
    if isinstance(payload, dict):
        outlier_ids = payload.get("outlier_ticket_ids") or []
        outlier_count = int(payload.get("outlier_count") or 0)
        outlier_suffix = f" ({', '.join(outlier_ids)})" if outlier_ids else ""
        bullets: list[str] = [
            f"In **{payload.get('window') or 'the selected window'}**, there were **{int(payload.get('total_records') or 0)}** system-updated RTN/CR record(s) totaling **{_format_money(payload.get('credit_total'))}** with an average of **{float(payload.get('avg_days_to_system_credit') or 0.0):.1f}** day(s) from entry to system credit.",
            f"Median time to system credit is **{float(payload.get('median_days_to_system_credit') or 0.0):.1f}** day(s); outlier tickets total **{outlier_count}**{outlier_suffix}." if outlier_count else f"Median time to system credit is **{float(payload.get('median_days_to_system_credit') or 0.0):.1f}** day(s); no timing outlier tickets were flagged.",
            f"Batch update dates total **{int(payload.get('batch_dates') or 0)}**, with **{int(payload.get('batched_dates') or 0)}** multi-record batch date(s) affecting **{int(payload.get('batched_records') or 0)}** record(s) / **{_format_money(payload.get('batched_credit_total'))}**; largest system batch was **{int(payload.get('largest_batch_count') or 0)}** record(s) on **{payload.get('largest_batch_date') or 'N/A'}** totaling **{_format_money(payload.get('largest_batch_credit_total'))}**.",
        ]
        manual_count = int(payload.get("manual_record_count") or 0)
        if manual_count:
            manual_outlier_ids = payload.get("manual_outlier_ticket_ids") or []
            manual_outlier_count = int(payload.get("manual_outlier_count") or 0)
            manual_suffix = f" ({', '.join(manual_outlier_ids)})" if manual_outlier_ids else ""
            bullets.append(
                f"Manual RTN-provided updates totaled **{manual_count}** record(s) / **{_format_money(payload.get('manual_credit_total'))}**, averaging **{float(payload.get('manual_avg_days_to_update') or 0.0):.1f}** day(s) from entry to RTN assignment; manual outlier tickets total **{manual_outlier_count}**{manual_suffix}." if manual_outlier_count else f"Manual RTN-provided updates totaled **{manual_count}** record(s) / **{_format_money(payload.get('manual_credit_total'))}**, averaging **{float(payload.get('manual_avg_days_to_update') or 0.0):.1f}** day(s) from entry to RTN assignment."
            )
            bullets.append(
                f"Manual multi-record batches affected **{int(payload.get('manual_batched_records') or 0)}** record(s) / **{_format_money(payload.get('manual_batched_credit_total'))}**; largest manual batch was **{int(payload.get('manual_largest_batch_count') or 0)}** record(s) on **{payload.get('manual_largest_batch_date') or 'N/A'}** totaling **{_format_money(payload.get('manual_largest_batch_credit_total'))}**."
            )
            return bullets[:4]
        return bullets[:3]
    return _generic_text_bullets(text, max_items=4)


def _billing_queue_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("billing_queue_hotspots")
    if not isinstance(payload, dict):
        return []
    bullets = [
        f"In **{payload.get('window') or 'the selected window'}**, billing queue delay affects **{int(payload.get('record_count') or 0)}** record(s) totaling **{_format_money(payload.get('credit_total'))}**.",
    ]
    top_customers = payload.get("top_customers") or []
    top_items = payload.get("top_items") or []
    if top_customers:
        lead = top_customers[0]
        if isinstance(lead, dict):
            bullets.append(
                f"Top customer hotspot is **{lead.get('label') or 'N/A'}** with **{int(lead.get('record_count') or 0)}** delayed record(s) / **{_format_money(lead.get('credit_total'))}**."
            )
    if top_items:
        lead = top_items[0]
        if isinstance(lead, dict):
            bullets.append(
                f"Top item hotspot is **{lead.get('label') or 'N/A'}** with **{int(lead.get('record_count') or 0)}** delayed record(s) / **{_format_money(lead.get('credit_total'))}**."
            )
    return bullets[:3]


def _root_cause_rtn_timing_bullets(meta: dict[str, Any]) -> list[str]:
    payload = meta.get("root_cause_rtn_timing")
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") or []
    bullets = [
        f"Across **{int(payload.get('record_count') or 0)}** credited record(s) in **{payload.get('window') or 'the selected window'}**, these root causes are taking the longest to reach RTN assignment.",
    ]
    for item in data[:3]:
        if not isinstance(item, dict):
            continue
        bullets.append(
            f"**{item.get('root_cause') or 'Unspecified'}** averages **{float(item.get('avg_days_to_rtn') or 0.0):.1f}** day(s) to RTN across **{int(item.get('record_count') or 0)}** record(s)."
        )
    return bullets[:4]


def _specialist_bullets(run: SpecialistRun) -> list[str]:
    bullets: list[str] = []
    if run.plan.id == "ticket_analysis":
        bullets = _ticket_bullets(run.meta)
    elif run.plan.id == "item_analysis":
        bullets = _item_bullets(run.meta)
    elif run.plan.id == "customer_analysis":
        bullets = _customer_bullets(run.meta)
    elif run.plan.id == "investigation_notes":
        bullets = _investigation_note_bullets(run.meta, run.text)
    elif run.plan.id == "credit_anomalies":
        bullets = _anomaly_bullets(run.meta, run.rows)
    elif run.plan.id == "credit_root_causes":
        bullets = _root_cause_bullets(run.meta)
    elif run.plan.id == "top_accounts":
        bullets = _top_accounts_bullets(run.meta)
    elif run.plan.id == "top_items":
        bullets = _top_items_bullets(run.meta)
    elif run.plan.id == "credit_trends":
        bullets = _trend_bullets(run.meta)
    elif run.plan.id == "credit_aging":
        bullets = _aging_bullets(run.text)
    elif run.plan.id == "credit_ops_snapshot":
        bullets = _ops_snapshot_bullets(run.text)
    elif run.plan.id == "system_updates":
        bullets = _system_updates_bullets(run.meta, run.text)
    elif run.plan.id == "billing_queue_hotspots":
        bullets = _billing_queue_bullets(run.meta)
    elif run.plan.id == "root_cause_rtn_timing":
        bullets = _root_cause_rtn_timing_bullets(run.meta)
    elif run.plan.id == "overall_summary":
        bullets = _overall_summary_bullets(run.meta, run.text)

    if bullets:
        return bullets
    return _generic_text_bullets(run.text, max_items=4)


def _specialist_headline(run: SpecialistRun) -> str:
    bullets = _specialist_bullets(run)
    if bullets:
        return bullets[0]
    return f"{run.plan.label} completed."


def _single_specialist_portfolio_executive_summary(run: SpecialistRun) -> str | None:
    if run.plan.id == "system_updates":
        payload = run.meta.get("system_updates_summary")
        if not isinstance(payload, dict):
            return None
        window = _humanize_window_label(payload.get("window") or "") or str(payload.get("window") or "the selected period").strip()
        total_records = int(payload.get("total_records") or 0)
        credit_total = float(payload.get("credit_total") or 0.0)
        median_days = float(payload.get("median_days_to_system_credit") or 0.0)
        batch_dates = int(payload.get("batch_dates") or 0)
        largest_batch_count = int(payload.get("largest_batch_count") or 0)
        largest_batch_date = str(payload.get("largest_batch_date") or "N/A").strip()
        largest_batch_credit_total = float(payload.get("largest_batch_credit_total") or 0.0)
        manual_count = int(payload.get("manual_record_count") or 0)
        manual_credit_total = float(payload.get("manual_credit_total") or 0.0)
        manual_avg_days = float(payload.get("manual_avg_days_to_update") or 0.0)
        parts = [
            f"In {window}, **{_format_count(total_records)}** system-updated RTN/CR record(s) totaling **{_format_money(credit_total)}** were processed.",
            f"Median time to system credit was **{median_days:.1f}** day(s), and **{_format_count(batch_dates)}** batch update date(s) were recorded; the largest batch was **{_format_count(largest_batch_count)}** record(s) on **{largest_batch_date}** totaling **{_format_money(largest_batch_credit_total)}**.",
        ]
        if manual_count:
            parts.append(
                f"Manual RTN-provided updates also covered **{_format_count(manual_count)}** record(s) / **{_format_money(manual_credit_total)}**, averaging **{manual_avg_days:.1f}** day(s) to RTN assignment."
            )
        return " ".join(parts)

    if run.plan.id == "top_accounts":
        payload = run.meta.get("top_accounts_summary")
        if not isinstance(payload, dict):
            return None
        data = [item for item in (payload.get("data") or []) if isinstance(item, dict)]
        if not data:
            return None
        window = _humanize_window_label(payload.get("window") or "") or str(payload.get("window") or "the selected period").strip()
        scope = str(payload.get("scope") or "credited").strip()
        total_credit = float(payload.get("total_credit") or 0.0)
        total_records = int(payload.get("total_record_count") or 0)
        lead = data[0]
        second = data[1] if len(data) > 1 else None
        top_two_total = float(lead.get("credit_total") or 0.0) + float(second.get("credit_total") or 0.0) if second else float(lead.get("credit_total") or 0.0)
        concentration_pct = (top_two_total / total_credit * 100.0) if total_credit > 0 else 0.0
        if scope == "open":
            first_sentence = (
                f"In {window}, **{_format_money(total_credit)}** remains open across **{_format_count(total_records)}** record(s)."
            )
            third_sentence = (
                f"These {'two customers' if second else 'customer'} account for **{concentration_pct:.1f}%** of total open exposure."
            )
        else:
            first_sentence = (
                f"In {window}, **{_format_money(total_credit)}** was credited across **{_format_count(total_records)}** record(s)."
            )
            third_sentence = (
                f"These {'two customers' if second else 'customer'} account for **{concentration_pct:.1f}%** of total credited volume."
            )
        second_sentence = (
            f"The top driver was **{lead.get('label') or 'N/A'}** at **{_format_money(lead.get('credit_total'))}** across **{_format_count(lead.get('record_count'))}** record(s)."
        )
        if second is not None:
            second_sentence += f" It was followed by **{second.get('label') or 'N/A'}** at **{_format_money(second.get('credit_total'))}**."
        return " ".join([first_sentence, second_sentence, third_sentence])

    if run.plan.id == "root_cause_rtn_timing":
        payload = run.meta.get("root_cause_rtn_timing")
        if not isinstance(payload, dict):
            return None
        data = [item for item in (payload.get("data") or []) if isinstance(item, dict)]
        if not data:
            return None
        window = _humanize_window_label(payload.get("window") or "") or str(payload.get("window") or "the selected period").strip()
        total_records = int(payload.get("record_count") or 0)
        lead = data[0]
        second = data[1] if len(data) > 1 else None
        third = data[2] if len(data) > 2 else None
        first_sentence = (
            f"In {window}, across **{_format_count(total_records)}** credited record(s), **{lead.get('root_cause') or 'Unspecified'}** is the slowest path to RTN assignment at **{float(lead.get('avg_days_to_rtn') or 0.0):.1f}** day(s) on average across **{_format_count(lead.get('record_count'))}** record(s)."
        )
        parts = [first_sentence]
        if second is not None:
            second_days = float(second.get("avg_days_to_rtn") or 0.0)
            ratio = (float(lead.get("avg_days_to_rtn") or 0.0) / second_days) if second_days > 0 else 0.0
            parts.append(
                f"That is nearly **{ratio:.1f}x** longer than **{second.get('root_cause') or 'Unspecified'}** at **{second_days:.1f}** day(s)."
            )
        if third is not None:
            parts.append(
                f"**{third.get('root_cause') or 'Unspecified'}** follows at **{float(third.get('avg_days_to_rtn') or 0.0):.1f}** day(s)."
            )
        return " ".join(parts)

    if run.plan.id == "billing_queue_hotspots":
        payload = run.meta.get("billing_queue_hotspots")
        if not isinstance(payload, dict):
            return None
        window = _humanize_window_label(payload.get("window") or "") or str(payload.get("window") or "the selected period").strip()
        top_customers = [item for item in (payload.get("top_customers") or []) if isinstance(item, dict)]
        top_items = [item for item in (payload.get("top_items") or []) if isinstance(item, dict)]
        parts = [
            f"In {window}, **{_format_count(payload.get('record_count'))}** record(s) totaling **{_format_money(payload.get('credit_total'))}** are currently delayed in the billing queue."
        ]
        if top_customers:
            lead_customer = top_customers[0]
            parts.append(
                f"Delay is concentrating in **{lead_customer.get('label') or 'N/A'}**, which accounts for **{_format_money(lead_customer.get('credit_total'))}** across **{_format_count(lead_customer.get('record_count'))}** record(s)."
            )
        if top_items:
            lead_item = top_items[0]
            parts.append(
                f"Top item hotspot is **{lead_item.get('label') or 'N/A'}** at **{_format_money(lead_item.get('credit_total'))}**."
            )
        return " ".join(parts)

    return None


def _build_specialist_sections(successful_runs: list[SpecialistRun]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for run in successful_runs:
        bullets = _specialist_bullets(run)
        if not bullets:
            bullets = ["No structured findings were available for this specialist."]
        sections.append(
            {
                "intent_id": run.plan.id,
                "label": run.plan.label,
                "bullets": bullets[:4],
            }
        )
    return sections


def _executive_summary(
    *,
    plan: AutoPlan,
    successful_runs: list[SpecialistRun],
    failed_runs: list[PlannedIntent],
) -> str:
    completed = len(successful_runs)
    requested = len(plan.intents)
    partial_note = ""
    if failed_runs:
        partial_note = f" One specialist did not complete ({', '.join(item.label for item in failed_runs)})." if len(failed_runs) == 1 else f" {len(failed_runs)} specialists did not complete."

    if not successful_runs:
        return "Auto Mode could not complete any specialist runs and fell back to the standard intent flow."

    if plan.family == AUTO_FAMILY_ENTITY:
        target = f" `{plan.target_label}`" if plan.target_label else ""
        first = successful_runs[0]
        return (
            f"Auto Mode reviewed{target} across **{completed}/{requested}** specialist run(s). "
            f"{_strip_markdown(_specialist_headline(first))}{partial_note}"
        )

    if len(successful_runs) == 1:
        single_summary = _single_specialist_portfolio_executive_summary(successful_runs[0])
        if single_summary:
            return single_summary + partial_note

    insight_bits = [_strip_markdown(_specialist_headline(run)) for run in successful_runs[:3]]
    joined = " ".join(insight_bits)
    return (
        f"Auto Mode ran **{completed}/{requested}** portfolio specialist(s) for the requested scope. "
        f"{joined}{partial_note}"
    )


def _render_follow_up_links(suggestions: list[dict[str, str]]) -> list[str]:
    lines: list[str] = []
    for item in suggestions:
        label = str(item.get("label") or "").strip()
        prefix = str(item.get("prefix") or "").strip()
        if not label or not prefix:
            continue
        lines.append(f"- `ask:{quote(prefix)}|{label}`")
    return lines


def _render_auto_answer(
    *,
    executive_summary: str,
    specialist_sections: list[dict[str, Any]],
    failed_runs: list[PlannedIntent],
    suggestions: list[dict[str, str]],
) -> str:
    lines = [
        "## Executive Summary",
        executive_summary,
        "",
        "## Key Findings By Specialist",
    ]

    for section in specialist_sections:
        label = str(section.get("label") or "").strip() or "Specialist"
        lines.append(f"### {label}")
        bullets = [str(item).strip() for item in (section.get("bullets") or []) if str(item).strip()]
        if not bullets:
            bullets = ["No structured findings were available for this specialist."]
        for bullet in bullets:
            lines.append(f"- {bullet}")
        lines.append("")

    if failed_runs:
        lines.append("### Execution Status")
        for item in failed_runs:
            lines.append(f"- {item.label} did not complete during this Auto run.")
        lines.append("")

    lines.append("## Recommended Follow-Ups")
    follow_up_lines = _render_follow_up_links(suggestions)
    if follow_up_lines:
        lines.extend(follow_up_lines)
    else:
        lines.append("- No additional follow-ups were suggested for this request.")

    return "\n".join(lines).strip()


def _render_portfolio_credit_brief(
    *,
    plan: AutoPlan,
    successful_runs: list[SpecialistRun],
    failed_runs: list[PlannedIntent],
    suggestions: list[dict[str, str]],
) -> str | None:
    if plan.family != AUTO_FAMILY_PORTFOLIO:
        return None

    ids = [run.plan.id for run in successful_runs]
    if set(ids) != {"overall_summary", "system_updates", "credit_root_causes"} or len(ids) != 3:
        return None

    run_by_id = {run.plan.id: run for run in successful_runs}
    overall_payload = run_by_id["overall_summary"].meta.get("overall_summary")
    system_payload = run_by_id["system_updates"].meta.get("system_updates_summary")
    root_payload = run_by_id["credit_root_causes"].meta.get("rootCauses")
    if not isinstance(overall_payload, dict) or not isinstance(system_payload, dict) or not isinstance(root_payload, dict):
        return None

    credited = overall_payload.get("credited_in_period")
    if not isinstance(credited, dict):
        credited = {}

    window = str(overall_payload.get("window") or system_payload.get("window") or root_payload.get("period") or "the selected period").strip()
    human_window = _humanize_window_label(window) or window

    system_outlier_ids = [str(item).strip() for item in (system_payload.get("outlier_ticket_ids") or []) if str(item).strip()]
    manual_outlier_ids = [str(item).strip() for item in (system_payload.get("manual_outlier_ticket_ids") or []) if str(item).strip()]

    lines = [
        "## Executive Summary",
        f"Period: {human_window}",
        "",
        "### Section 1: Volume & Activity",
        (
            f"In the period {human_window}, **{_format_count(credited.get('credited_record_count'))}** unique RTN record(s) "
            f"were credited totaling **{_format_money(credited.get('credited_credit_total'))}**. "
            f"Of those, **{_format_count(overall_payload.get('open_record_count'))}** record(s) totaling "
            f"**{_format_money(overall_payload.get('open_credit_total'))}** remain open."
        ),
        "",
        "### Section 2: Time-to-Resolution",
        (
            f"Average time from entry to credit was **{float(credited.get('avg_days_to_rtn_assignment') or 0.0):.1f}** day(s). "
            f"For system-processed records, the average was **{float(system_payload.get('avg_days_to_system_credit') or 0.0):.1f}** day(s) "
            f"with a median of **{float(system_payload.get('median_days_to_system_credit') or 0.0):.1f}** day(s). "
            f"For manually assigned records, the average from entry to RTN assignment was "
            f"**{float(system_payload.get('manual_avg_days_to_update') or 0.0):.1f}** day(s)."
        ),
    ]

    if int(system_payload.get("outlier_count") or 0) > 0:
        lines.extend(
            [
                "",
                f"System outliers ({int(system_payload.get('outlier_count') or 0)}):",
                ", ".join(system_outlier_ids),
            ]
        )
    if int(system_payload.get("manual_outlier_count") or 0) > 0:
        lines.extend(
            [
                "",
                f"Manual outliers ({int(system_payload.get('manual_outlier_count') or 0)}):",
                ", ".join(manual_outlier_ids),
            ]
        )
    lines.append("")
    lines.append(
        (
            f"Processing note: **{_format_count(system_payload.get('batch_dates'))}** batch update date(s) were recorded, "
            f"with **{_format_count(system_payload.get('batched_dates'))}** multi-record batch(es) affecting "
            f"**{_format_count(system_payload.get('batched_records'))}** record(s) / **{_format_money(system_payload.get('batched_credit_total'))}**; "
            f"the largest single batch was **{_format_count(system_payload.get('largest_batch_count'))}** record(s) on "
            f"**{system_payload.get('largest_batch_date') or 'N/A'}** totaling **{_format_money(system_payload.get('largest_batch_credit_total'))}**."
        )
    )
    lines.extend(
        [
            "",
            "### Section 3: Open Exposure",
            (
                f"**{_format_count(overall_payload.get('open_record_count'))}** record(s) totaling "
                f"**{_format_money(overall_payload.get('open_credit_total'))}** remain open. "
                f"Average open age is **{float(overall_payload.get('avg_days_open') or 0.0):.1f}** day(s), with "
                f"**{float(overall_payload.get('avg_days_since_last_status') or 0.0):.1f}** day(s) since the last update."
            ),
            (
                f"Billing queue delay affects **{_format_count(overall_payload.get('billing_queue_delay_count'))}** record(s) totaling "
                f"**{_format_money(overall_payload.get('billing_queue_delay_total'))}**, and stale investigation affects "
                f"**{_format_count(overall_payload.get('stale_investigation_count'))}** record(s) totaling "
                f"**{_format_money(overall_payload.get('stale_investigation_total'))}**."
            ),
            f"Reopened after terminal: **{_format_count(credited.get('reopened_after_terminal_count'))}** record(s).",
            "",
            "### Section 4: Root Causes",
            f"Total exposure across root-cause groups is **{root_payload.get('total') or 'N/A'}**.",
            "",
        ]
    )

    for item in (root_payload.get("data") or [])[:3]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"- **{item.get('root_cause') or 'Unspecified'}**: **{_format_money(item.get('credit_request_total'))}** across **{_format_count(item.get('record_count'))}** record(s)"
        )

    if failed_runs:
        lines.extend(["", "### Execution Status"])
        for item in failed_runs:
            lines.append(f"- {item.label} did not complete during this Auto run.")

    lines.extend(["", "## Recommended Follow-Ups"])
    follow_up_lines = _render_follow_up_links(suggestions)
    if follow_up_lines:
        lines.extend(follow_up_lines)
    else:
        lines.append("- No additional follow-ups were suggested for this request.")

    return "\n".join(lines).strip()


def _render_overall_summary_brief(
    *,
    plan: AutoPlan,
    successful_runs: list[SpecialistRun],
    failed_runs: list[PlannedIntent],
    suggestions: list[dict[str, str]],
) -> str | None:
    if plan.family != AUTO_FAMILY_PORTFOLIO:
        return None
    if len(successful_runs) != 1 or successful_runs[0].plan.id != "overall_summary":
        return None

    payload = successful_runs[0].meta.get("overall_summary")
    if not isinstance(payload, dict):
        return None
    credited = payload.get("credited_in_period")
    if not isinstance(credited, dict):
        credited = {}

    window = str(payload.get("window") or "the selected period").strip()
    human_window = _humanize_window_label(window) or window

    manual_count = _format_count(credited.get("primary_manual_record_count"))
    manual_total = _format_money(credited.get("primary_manual_credit_total"))
    system_count = _format_count(credited.get("primary_system_record_count"))
    system_total = _format_money(credited.get("primary_system_credit_total"))

    if float(credited.get("primary_manual_credit_total") or 0.0) == 0.0:
        attribution_line = (
            f"All credited activity in the period was system-led: **{system_count}** record(s) / **{system_total}**; "
            f"manual-led activity was **{manual_count}** record(s) / **{manual_total}**."
        )
    else:
        attribution_line = (
            f"Primary attribution was system-led **{system_count}** record(s) / **{system_total}** and "
            f"manual-led **{manual_count}** record(s) / **{manual_total}**."
        )

    lines = [
        "## Executive Summary",
        "",
        "### Section 1: Period Activity",
        (
            f"In {human_window}, **{_format_count(credited.get('credited_record_count'))}** unique record(s) were credited totaling "
            f"**{_format_money(credited.get('credited_credit_total'))}**. During the same period, "
            f"**{_format_count(payload.get('open_record_count'))}** record(s) remained open totaling "
            f"**{_format_money(payload.get('open_credit_total'))}**."
        ),
        "",
        "### Section 2: Time-to-Resolution",
        (
            f"Average open age was **{float(payload.get('avg_days_open') or 0.0):.1f}** day(s), with "
            f"**{float(payload.get('avg_days_since_last_status') or 0.0):.1f}** day(s) since the last update. "
            f"Average time to RTN assignment was **{float(credited.get('avg_days_to_rtn_assignment') or 0.0):.1f}** day(s)."
        ),
        "",
        "### Section 3: Open Exposure",
        (
            f"Open exposure totaled **{_format_money(payload.get('open_credit_total'))}** across "
            f"**{_format_count(payload.get('open_record_count'))}** record(s). "
            f"Billing queue delay affects **{_format_count(payload.get('billing_queue_delay_count'))}** record(s) totaling "
            f"**{_format_money(payload.get('billing_queue_delay_total'))}**, and stale investigation affects "
            f"**{_format_count(payload.get('stale_investigation_count'))}** record(s) totaling "
            f"**{_format_money(payload.get('stale_investigation_total'))}**."
        ),
        f"Reopened after terminal totals **{_format_count(credited.get('reopened_after_terminal_count'))}** record(s).",
        "",
        "### Section 4: Attribution",
        attribution_line,
    ]

    if failed_runs:
        lines.extend(["", "### Execution Status"])
        for item in failed_runs:
            lines.append(f"- {item.label} did not complete during this Auto run.")

    lines.extend(["", "## Recommended Follow-Ups"])
    follow_up_lines = _render_follow_up_links(suggestions)
    if follow_up_lines:
        lines.extend(follow_up_lines)
    else:
        lines.append("- No additional follow-ups were suggested for this request.")

    return "\n".join(lines).strip()


def _render_overview_trends_brief(
    *,
    plan: AutoPlan,
    successful_runs: list[SpecialistRun],
    failed_runs: list[PlannedIntent],
    suggestions: list[dict[str, str]],
) -> str | None:
    if plan.family != AUTO_FAMILY_PORTFOLIO:
        return None
    ids = [run.plan.id for run in successful_runs]
    if set(ids) != {"overall_summary", "credit_trends"} or len(ids) != 2:
        return None

    run_by_id = {run.plan.id: run for run in successful_runs}
    overview_payload = run_by_id["overall_summary"].meta.get("overall_summary")
    trend_payload = run_by_id["credit_trends"].meta.get("creditTrends")
    if not isinstance(overview_payload, dict) or not isinstance(trend_payload, dict):
        return None

    credited = overview_payload.get("credited_in_period")
    if not isinstance(credited, dict):
        credited = {}

    window = str(overview_payload.get("window") or "the selected period").strip()
    human_window = _humanize_window_label(window) or window

    manual_count = _format_count(credited.get("primary_manual_record_count"))
    manual_total = _format_money(credited.get("primary_manual_credit_total"))
    system_count = _format_count(credited.get("primary_system_record_count"))
    system_total = _format_money(credited.get("primary_system_credit_total"))

    if float(credited.get("primary_manual_credit_total") or 0.0) == 0.0:
        attribution_line = (
            f"All credited activity in the period was system-led: **{system_count}** record(s) / **{system_total}**; "
            f"manual-led activity was **{manual_count}** record(s) / **{manual_total}**."
        )
    else:
        attribution_line = (
            f"Primary attribution was system-led **{system_count}** record(s) / **{system_total}** and "
            f"manual-led **{manual_count}** record(s) / **{manual_total}**."
        )

    trend_metrics = trend_payload.get("metrics") if isinstance(trend_payload.get("metrics"), list) else []
    total_credit_metric = next(
        (item for item in trend_metrics if isinstance(item, dict) and str(item.get("label") or "").strip().lower() == "total credits"),
        None,
    )
    volume_metric = next(
        (item for item in trend_metrics if isinstance(item, dict) and str(item.get("label") or "").strip().lower() == "volume (rows)"),
        None,
    )
    trend_lead_parts = [
        (
            f"In {human_window}, **{_format_money(credited.get('credited_credit_total'))}** was credited across "
            f"**{_format_count(credited.get('credited_record_count'))}** unique record(s), while "
            f"**{_format_count(overview_payload.get('open_record_count'))}** record(s) totaling "
            f"**{_format_money(overview_payload.get('open_credit_total'))}** remain open."
        )
    ]
    if isinstance(total_credit_metric, dict):
        credit_change = float(total_credit_metric.get("change") or 0.0)
        direction = "up" if credit_change > 0 else "down"
        sentence = f"Compared with the prior period, total credits were **{direction} {abs(credit_change):.1f}%**"
        if isinstance(volume_metric, dict):
            volume_change = float(volume_metric.get("change") or 0.0)
            volume_direction = "up" if volume_change > 0 else "down"
            sentence += f" and volume was **{volume_direction} {abs(volume_change):.1f}%**."
        else:
            sentence += "."
        trend_lead_parts.append(sentence)

    lines = [
        "## Executive Summary",
        "",
        " ".join(trend_lead_parts),
        "",
        "### Section 1: Period Activity",
        (
            f"In {human_window}, **{_format_count(credited.get('credited_record_count'))}** unique record(s) were credited totaling "
            f"**{_format_money(credited.get('credited_credit_total'))}**. During the same period, "
            f"**{_format_count(overview_payload.get('open_record_count'))}** record(s) remained open totaling "
            f"**{_format_money(overview_payload.get('open_credit_total'))}**."
        ),
        "",
        "### Section 2: Time-to-Resolution",
        (
            f"Average open age was **{float(overview_payload.get('avg_days_open') or 0.0):.1f}** day(s), with "
            f"**{float(overview_payload.get('avg_days_since_last_status') or 0.0):.1f}** day(s) since the last update. "
            f"Average time to RTN assignment was **{float(credited.get('avg_days_to_rtn_assignment') or 0.0):.1f}** day(s)."
        ),
        "",
        "### Section 3: Open Exposure",
        (
            f"Open exposure totaled **{_format_money(overview_payload.get('open_credit_total'))}** across "
            f"**{_format_count(overview_payload.get('open_record_count'))}** record(s). "
            f"Billing queue delay affects **{_format_count(overview_payload.get('billing_queue_delay_count'))}** record(s) totaling "
            f"**{_format_money(overview_payload.get('billing_queue_delay_total'))}**, and stale investigation affects "
            f"**{_format_count(overview_payload.get('stale_investigation_count'))}** record(s) totaling "
            f"**{_format_money(overview_payload.get('stale_investigation_total'))}**."
        ),
        f"Reopened after terminal totals **{_format_count(credited.get('reopened_after_terminal_count'))}** record(s).",
        "",
        "### Section 4: Attribution",
        attribution_line,
        "",
        "### Section 5: Trends",
    ]

    metrics = trend_payload.get("metrics")
    if isinstance(metrics, list):
        for metric in metrics[:3]:
            if not isinstance(metric, dict):
                continue
            current = metric.get("current")
            previous = metric.get("previous")
            change = metric.get("change")
            is_currency = bool(metric.get("isCurrency"))
            current_text = _format_money(current) if is_currency else str(current)
            previous_text = _format_money(previous) if is_currency else str(previous)
            lines.append(
                f"- **{metric.get('label') or 'Metric'}**: {current_text} vs {previous_text} ({float(change or 0):+.1f}%)."
            )

    trend_window = trend_payload.get("window")
    if isinstance(trend_window, dict) and trend_window.get("current") and trend_window.get("previous"):
        lines.append(
            f"- Comparison window: **{trend_window.get('previous')}** against **{trend_window.get('current')}**."
        )

    if failed_runs:
        lines.extend(["", "### Execution Status"])
        for item in failed_runs:
            lines.append(f"- {item.label} did not complete during this Auto run.")

    lines.extend(["", "## Recommended Follow-Ups"])
    follow_up_lines = _render_follow_up_links(suggestions)
    if follow_up_lines:
        lines.extend(follow_up_lines)
    else:
        lines.append("- No additional follow-ups were suggested for this request.")

    return "\n".join(lines).strip()


def _deterministic_auto_answer(
    *,
    query: str,
    plan: AutoPlan,
    successful_runs: list[SpecialistRun],
    failed_runs: list[PlannedIntent],
    suggestions: list[dict[str, str]],
) -> str:
    return _render_auto_answer(
        executive_summary=_executive_summary(
            plan=plan,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
        ),
        specialist_sections=_build_specialist_sections(successful_runs),
        failed_runs=failed_runs,
        suggestions=suggestions,
    )


def _llm_synthesize_auto_answer(
    *,
    query: str,
    plan: AutoPlan,
    successful_runs: list[SpecialistRun],
    failed_runs: list[PlannedIntent],
    suggestions: list[dict[str, str]],
) -> str | None:
    specialist_sections = _build_specialist_sections(successful_runs)
    system_prompt = (
        "You synthesize specialist outputs for Actus Auto Mode, an internal credit operations assistant. "
        "Return ONLY valid JSON with keys `executive_summary` and `specialists`. "
        "`executive_summary` must be a concise natural-language summary, 2-4 sentences max. "
        "`specialists` must be an array aligned to the input specialist order; each item must have `bullets`, "
        "a list of 2-4 concise grounded markdown bullet strings. "
        "Use only the provided findings. Do not invent facts, causes, amounts, or follow-ups. "
        "Do not output note IDs, action tokens, or raw commands unless they are essential evidence."
    )
    user_prompt = json.dumps(
        {
            "query": query,
            "plan": {
                "family": plan.family,
                "primary_intent": plan.primary_intent,
                "target_label": plan.target_label,
            },
            "successful_specialists": specialist_sections,
            "failed_specialists": [
                {
                    "id": item.id,
                    "label": item.label,
                }
                for item in failed_runs
            ],
            "follow_up_labels": [str(item.get("label") or "").strip() for item in suggestions if item.get("label")],
        }
    )

    primary_model, fallback_model = _resolve_auto_summary_models()
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        raw = openrouter_chat(messages, model=primary_model) if primary_model else openrouter_chat(messages)
        _resolve_auto_summary_primary_model_name(primary_model)
    except Exception:
        if not fallback_model:
            return None
        try:
            raw = openrouter_chat(messages, model=fallback_model)
        except Exception:
            return None

    try:
        payload = json.loads(_clean_llm_json(raw))
    except Exception:
        return None

    executive_summary = str(payload.get("executive_summary") or "").strip()
    specialist_payload = payload.get("specialists")
    if not executive_summary or not isinstance(specialist_payload, list):
        return None

    rendered_sections: list[dict[str, Any]] = []
    for index, base in enumerate(specialist_sections):
        section = specialist_payload[index] if index < len(specialist_payload) and isinstance(specialist_payload[index], dict) else {}
        raw_bullets = section.get("bullets")
        bullets = [str(item).strip() for item in raw_bullets if str(item).strip()] if isinstance(raw_bullets, list) else []
        if not bullets:
            bullets = list(base["bullets"])
        rendered_sections.append(
            {
                "intent_id": base["intent_id"],
                "label": base["label"],
                "bullets": bullets[:4],
            }
        )

    return _render_auto_answer(
        executive_summary=executive_summary,
        specialist_sections=rendered_sections,
        failed_runs=failed_runs,
        suggestions=suggestions,
    )


def auto_mode_answer(query: str, df: pd.DataFrame) -> tuple[str, pd.DataFrame | None, dict[str, Any]]:
    working_df = df.copy(deep=False)
    if isinstance(getattr(df, "attrs", None), dict):
        working_df.attrs = dict(df.attrs)
    working_df.attrs["_actus_intent_cache"] = {}

    plan = plan_auto_mode(query)
    if plan is None or not plan.intents:
        return actus_answer(query, working_df)

    successful_runs: list[SpecialistRun] = []
    failed_runs: list[PlannedIntent] = []
    executed_intents: list[dict[str, str]] = []

    for planned_intent in plan.intents:
        try:
            run = _execute_planned_intent(planned_intent, working_df)
        except Exception:
            LOGGER.exception("auto_mode specialist failed: %s", planned_intent.id)
            failed_runs.append(planned_intent)
            executed_intents.append(
                {
                    "id": planned_intent.id,
                    "label": planned_intent.label,
                    "status": "error",
                }
            )
            continue

        successful_runs.append(run)
        executed_intents.append(
            {
                "id": planned_intent.id,
                "label": planned_intent.label,
                "status": "ok",
            }
        )

    if not successful_runs:
        return actus_answer(query, working_df)

    result_suggestions = [
        item
        for run in successful_runs
        if isinstance(run.meta.get("suggestions"), list)
        for item in run.meta["suggestions"]
        if isinstance(item, dict)
    ]
    suggestions = _merge_suggestions(list(plan.suggestions), result_suggestions)

    text = _render_portfolio_credit_brief(
        plan=plan,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        suggestions=suggestions,
    )
    if text is None:
        text = _render_overall_summary_brief(
            plan=plan,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            suggestions=suggestions,
        )
    if text is None:
        text = _render_overview_trends_brief(
            plan=plan,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            suggestions=suggestions,
        )
    if text is None:
        if plan.family == AUTO_FAMILY_PORTFOLIO:
            text = _deterministic_auto_answer(
                query=query,
                plan=plan,
                successful_runs=successful_runs,
                failed_runs=failed_runs,
                suggestions=suggestions,
            )
        else:
            text = _llm_synthesize_auto_answer(
                query=query,
                plan=plan,
                successful_runs=successful_runs,
                failed_runs=failed_runs,
                suggestions=suggestions,
            ) or _deterministic_auto_answer(
                query=query,
                plan=plan,
                successful_runs=successful_runs,
                failed_runs=failed_runs,
                suggestions=suggestions,
            )

    meta: dict[str, Any] = {
        "intent_id": "auto_mode",
        "intent": "auto_mode",
        "show_table": False,
        "suggestions": suggestions,
        "auto_mode": {
            "enabled": True,
            "planner": "deterministic_first",
            "primary_intent": plan.primary_intent,
            "executed_intents": executed_intents,
            "subintent_count": len(successful_runs),
        },
    }
    return text, None, meta
