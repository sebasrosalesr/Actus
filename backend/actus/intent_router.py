from typing import Callable, Dict, List, Tuple, Optional
import json
import os
import re
from datetime import datetime
from difflib import SequenceMatcher
import pandas as pd

from actus.intents.ticket_status import intent_ticket_status, INTENT_ALIASES as TICKET_STATUS_ALIASES
from actus.intents.record_lookup import intent_record_lookup, INTENT_ALIASES as RECORD_LOOKUP_ALIASES
from actus.intents.mixed_lines import intent_mixed_lines, INTENT_ALIASES as MIXED_LINES_ALIASES
from actus.intents.customer_tickets import intent_customer_tickets, INTENT_ALIASES as CUSTOMER_TICKETS_ALIASES
from actus.intents.credit_activity import intent_credit_activity, INTENT_ALIASES as CREDIT_ACTIVITY_ALIASES
from actus.intents.credit_numbers import intent_rtn_summary, INTENT_ALIASES as CREDIT_NUMBERS_ALIASES
from actus.intents.priority_tickets import intent_priority_tickets, INTENT_ALIASES as PRIORITY_TICKETS_ALIASES
from actus.intents.credit_aging import intent_credit_aging, INTENT_ALIASES as CREDIT_AGING_ALIASES
from actus.intents.stalled_tickets import intent_stalled_tickets, INTENT_ALIASES as STALLED_TICKETS_ALIASES
from actus.intents.overall_summary import intent_overall_summary, INTENT_ALIASES as OVERALL_SUMMARY_ALIASES
from actus.intents.top_accounts import intent_top_accounts, INTENT_ALIASES as TOP_ACCOUNTS_ALIASES
from actus.intents.top_salesreps import intent_top_salesreps, INTENT_ALIASES as TOP_SALESREPS_ALIASES
from actus.intents.top_items import intent_top_items, INTENT_ALIASES as TOP_ITEMS_ALIASES
from actus.intents.credit_trends import intent_credit_trends, INTENT_ALIASES as CREDIT_TRENDS_ALIASES
from actus.intents.credit_anomalies import intent_credit_anomalies, INTENT_ALIASES as CREDIT_ANOMALIES_ALIASES
from actus.intents.ticket_requests import intent_ticket_requests, INTENT_ALIASES as TICKET_REQUESTS_ALIASES
from actus.intents.investigation_notes import intent_investigation_notes, INTENT_ALIASES as INVESTIGATION_NOTES_ALIASES
from actus.intents.system_updates import intent_system_updates, INTENT_ALIASES as SYSTEM_UPDATES_ALIASES
from actus.intents.credit_ops_snapshot import intent_credit_ops_snapshot, INTENT_ALIASES as CREDIT_OPS_SNAPSHOT_ALIASES
from actus.intents.credit_amount_plot import intent_credit_amount_plot, INTENT_ALIASES as CREDIT_AMOUNT_PLOT_ALIASES
from actus.intents.credit_root_causes import intent_root_cause_summary, INTENT_ALIASES as CREDIT_ROOT_CAUSES_ALIASES
from actus.intents.bulk_search import intent_bulk_search, INTENT_ALIASES as BULK_SEARCH_ALIASES
from actus.help_text import HELP_TEXT
from actus.openrouter_client import openrouter_chat

# --------------------------------------------------
# INTENT LIST
# --------------------------------------------------
INTENTS: List[Callable[[str, pd.DataFrame], Optional[Tuple[str, Optional[pd.DataFrame]]]]] = [
    intent_ticket_status,
    intent_ticket_requests,    # NEW — returns (text, df)
    intent_bulk_search,
    intent_record_lookup,
    intent_mixed_lines,
    intent_customer_tickets,
    intent_credit_activity,
    intent_rtn_summary,
    intent_priority_tickets,
    intent_credit_aging,
    intent_stalled_tickets,
    intent_overall_summary,
    intent_root_cause_summary,
    intent_top_accounts,
    intent_top_salesreps,
    intent_top_items,
    intent_credit_trends,
    intent_credit_anomalies,
    intent_system_updates,
    intent_credit_ops_snapshot,
    intent_credit_amount_plot,
    intent_investigation_notes,
]

INTENT_DEFS = [
    {"id": "ticket_status", "label": "Ticket status", "prefix": "ticket status", "func": intent_ticket_status, "aliases": TICKET_STATUS_ALIASES},
    {"id": "ticket_requests", "label": "Ticket requests", "prefix": "ticket requests", "func": intent_ticket_requests, "aliases": TICKET_REQUESTS_ALIASES},
    {"id": "bulk_search", "label": "Bulk search", "prefix": "bulk search", "func": intent_bulk_search, "aliases": BULK_SEARCH_ALIASES},
    {"id": "record_lookup", "label": "Record lookup", "prefix": "record lookup", "func": intent_record_lookup, "aliases": RECORD_LOOKUP_ALIASES},
    {"id": "mixed_lines", "label": "Mixed lines", "prefix": "mixed lines", "func": intent_mixed_lines, "aliases": MIXED_LINES_ALIASES},
    {"id": "customer_tickets", "label": "Customer history", "prefix": "customer tickets", "func": intent_customer_tickets, "aliases": CUSTOMER_TICKETS_ALIASES},
    {"id": "credit_activity", "label": "Credit activity", "prefix": "credit activity", "func": intent_credit_activity, "aliases": CREDIT_ACTIVITY_ALIASES},
    {"id": "credit_numbers", "label": "Credits with RTN", "prefix": "credits with rtn", "func": intent_rtn_summary, "aliases": CREDIT_NUMBERS_ALIASES},
    {"id": "priority_tickets", "label": "Priority tickets", "prefix": "priority tickets", "func": intent_priority_tickets, "aliases": PRIORITY_TICKETS_ALIASES},
    {"id": "credit_aging", "label": "Credit aging", "prefix": "credit aging", "func": intent_credit_aging, "aliases": CREDIT_AGING_ALIASES},
    {"id": "stalled_tickets", "label": "Stalled tickets", "prefix": "stalled tickets", "func": intent_stalled_tickets, "aliases": STALLED_TICKETS_ALIASES},
    {"id": "overall_summary", "label": "Credit overview", "prefix": "credit overview", "func": intent_overall_summary, "aliases": OVERALL_SUMMARY_ALIASES},
    {"id": "credit_root_causes", "label": "Root causes", "prefix": "root causes", "func": intent_root_cause_summary, "aliases": CREDIT_ROOT_CAUSES_ALIASES},
    {"id": "top_accounts", "label": "Top accounts", "prefix": "top accounts", "func": intent_top_accounts, "aliases": TOP_ACCOUNTS_ALIASES},
    {"id": "top_salesreps", "label": "Top sales reps", "prefix": "top sales reps", "func": intent_top_salesreps, "aliases": TOP_SALESREPS_ALIASES},
    {"id": "top_items", "label": "Top items", "prefix": "top items", "func": intent_top_items, "aliases": TOP_ITEMS_ALIASES},
    {"id": "credit_trends", "label": "Credit trends", "prefix": "credit trends", "func": intent_credit_trends, "aliases": CREDIT_TRENDS_ALIASES},
    {"id": "credit_anomalies", "label": "Credit anomalies", "prefix": "credit anomalies", "func": intent_credit_anomalies, "aliases": CREDIT_ANOMALIES_ALIASES},
    {"id": "system_updates", "label": "System updates with RTN", "prefix": "system updates", "func": intent_system_updates, "aliases": SYSTEM_UPDATES_ALIASES},
    {"id": "credit_ops_snapshot", "label": "Credit ops snapshot", "prefix": "credit ops snapshot", "func": intent_credit_ops_snapshot, "aliases": CREDIT_OPS_SNAPSHOT_ALIASES},
    {"id": "credit_amount_plot", "label": "Credit amount chart", "prefix": "credit amount chart", "func": intent_credit_amount_plot, "aliases": CREDIT_AMOUNT_PLOT_ALIASES},
    {"id": "investigation_notes", "label": "Investigation notes", "prefix": "investigation notes", "func": intent_investigation_notes, "aliases": INVESTIGATION_NOTES_ALIASES},
]


def _normalize_query(query: str) -> str:
    cleaned = query.lower()
    cleaned = re.sub(r"\bactus\b", "", cleaned)
    cleaned = re.sub(r"\bplease\b", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _alias_matches(query: str, alias: str) -> bool:
    if alias in query:
        return True
    query_words = query.split()
    alias_words = alias.split()
    if len(alias_words) > len(query_words) or not alias_words:
        return False
    n = len(alias_words)
    for i in range(len(query_words) - n + 1):
        window = " ".join(query_words[i:i + n])
        if SequenceMatcher(None, window, alias).ratio() >= 0.86:
            return True
    return False


def _match_intent_alias(query: str):
    normalized = _normalize_query(query)
    for item in INTENT_DEFS:
        for alias in item["aliases"]:
            if _alias_matches(normalized, alias):
                return item
    return None


def _is_help_query(query: str) -> bool:
    normalized = _normalize_query(query)
    if any(phrase in normalized for phrase in ["what can you do", "what do you do", "capabilities", "functions"]):
        return True
    return normalized in {"help", "help me", "help please"}


def _classify_intent_openrouter(query: str):
    enabled = os.environ.get("ACTUS_INTENT_CLASSIFIER", "").strip().lower() in {"1", "true", "yes"}
    if not enabled:
        return None
    fallback_model = os.environ.get("ACTUS_OPENROUTER_MODEL_FALLBACK")
    catalog = [
        {"id": item["id"], "label": item["label"]}
        for item in INTENT_DEFS
    ]
    system_prompt = (
        "You are an intent classifier. Return ONLY JSON with keys "
        "`intent` (string) and `confidence` (0-1). "
        "Choose the best intent id from the provided list."
    )
    user_prompt = "Intents:\n" + json.dumps(catalog) + "\n\nQuery:\n" + query
    try:
        raw = openrouter_chat(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z]*", "", cleaned).strip()
            cleaned = cleaned.rstrip("`").strip()
        data = json.loads(cleaned)
        intent_id = data.get("intent")
        confidence = float(data.get("confidence", 0))
        if (not intent_id or confidence < 0.6) and fallback_model:
            raw = openrouter_chat(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                model=fallback_model,
            )
            cleaned = raw.strip()
            if cleaned.startswith("```"):
                cleaned = re.sub(r"^```[a-zA-Z]*", "", cleaned).strip()
                cleaned = cleaned.rstrip("`").strip()
            data = json.loads(cleaned)
            intent_id = data.get("intent")
            confidence = float(data.get("confidence", 0))
        if not intent_id or confidence < 0.6:
            return None
        for item in INTENT_DEFS:
            if item["id"] == intent_id:
                return item
    except Exception:
        return None
    return None


def _suggest_intents_openrouter(query: str):
    enabled = os.environ.get("ACTUS_INTENT_CLASSIFIER", "").strip().lower() in {"1", "true", "yes"}
    if not enabled:
        return []
    fallback_model = os.environ.get("ACTUS_OPENROUTER_MODEL_FALLBACK")
    catalog = [{"id": item["id"], "label": item["label"]} for item in INTENT_DEFS]
    system_prompt = (
        "You are an intent suggester. Return ONLY JSON as a list of up to 3 objects "
        "with keys `intent` (string) and `confidence` (0-1). "
        "Choose from the provided intent IDs."
    )
    user_prompt = "Intents:\n" + json.dumps(catalog) + "\n\nQuery:\n" + query

    def _call(model_override=None):
        return openrouter_chat(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            model=model_override,
        )

    try:
        raw = _call()
    except Exception:
        if fallback_model:
            raw = _call(model_override=fallback_model)
        else:
            return []
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*", "", cleaned).strip()
        cleaned = cleaned.rstrip("`").strip()
    try:
        data = json.loads(cleaned)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    suggestions = []
    for item in data[:3]:
        intent_id = item.get("intent") if isinstance(item, dict) else None
        confidence = float(item.get("confidence", 0)) if isinstance(item, dict) else 0
        if not intent_id:
            continue
        for entry in INTENT_DEFS:
            if entry["id"] == intent_id:
                suggestions.append({
                    "id": entry["id"],
                    "label": entry["label"],
                    "prefix": entry["prefix"],
                    "confidence": confidence,
                })
    return suggestions


# --------------------------------------------------
# ROUTER — ALWAYS RETURNS (text, df, meta)
# --------------------------------------------------
def actus_answer(query: str, df: pd.DataFrame) -> Tuple[str, Optional[pd.DataFrame], Dict[str, object]]:
    """
    Main orchestrator for Actus.
    Always returns:
        (text_response, df_result or None, meta)
    """

    q_low = query.lower()
    if _is_help_query(q_low):
        return (HELP_TEXT, None, {"is_help": True})

    normalized = _normalize_query(q_low)
    if any(term in normalized for term in ["plot", "chart", "graph"]) and "credit" not in normalized:
        return (
            "What date range should I use for the credit amount chart? "
            "Try: today, yesterday, last 7 days, last month, or a custom range.",
            None,
            {"follow_up": {"intent": "credit_amount_plot", "prefix": "credit amount chart"}},
        )

    if any(phrase in normalized for phrase in ["what day is today", "what day is it", "what is today", "today's date", "todays date"]):
        today = datetime.now().strftime("%A, %B %d, %Y")
        return (f"Today is {today}.", None, {})
    if (
        ("credit" in q_low or "credits" in q_low)
        and ("update" in q_low or "updated" in q_low)
        and any(k in q_low for k in ["today", "yesterday", "last ", "this week", "this month"])
    ):
        result = intent_credit_activity(query, df)
        if isinstance(result, tuple) and len(result) in {2, 3}:
            if len(result) == 2:
                return (result[0], result[1], {})
            return result

    if "investigation" in q_low and "note" in q_low:
        result = intent_investigation_notes(query, df)
        if isinstance(result, tuple) and len(result) in {2, 3}:
            if len(result) == 2:
                return (result[0], result[1], {})
            return result

    alias_match = _match_intent_alias(query)
    if alias_match:
        result = alias_match["func"](query, df)
        if isinstance(result, str):
            return (result, None, {})
        if isinstance(result, tuple) and len(result) == 2:
            return (result[0], result[1], {})
        if isinstance(result, tuple) and len(result) == 3:
            return result

    classifier_match = _classify_intent_openrouter(query)
    if classifier_match:
        result = classifier_match["func"](query, df)
        if isinstance(result, str):
            return (result, None, {})
        if isinstance(result, tuple) and len(result) == 2:
            return (result[0], result[1], {})
        if isinstance(result, tuple) and len(result) == 3:
            return result

    for intent in INTENTS:
        result = intent(query, df)

        if result is None:
            continue

        # Intent returned only a string
        if isinstance(result, str):
            return (result, None, {})

        # Intent returned (text, df)
        if isinstance(result, tuple) and len(result) == 2:
            return (result[0], result[1], {})

        # Intent returned (text, df, meta)
        if isinstance(result, tuple) and len(result) == 3:
            return result

    suggestions = _suggest_intents_openrouter(query)
    if suggestions:
        lines = ["I couldn't match that exactly. Did you mean:"]
        for idx, item in enumerate(suggestions, start=1):
            lines.append(f"{idx}. {item['label']}")
        lines.append("Reply with 1, 2, or 3.")
        return ("\n".join(lines), None, {"suggestions": suggestions})

    return (HELP_TEXT, None, {})
