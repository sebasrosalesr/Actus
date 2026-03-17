import re
import pandas as pd

from app.rag.new_design.service import get_runtime_service

INTENT_ALIASES = [
    "analyze customer",
    "customer analysis",
    "analyze account",
    "account analysis",
    "analyze account prefix",
]


def _is_explicit_analyze_query(query: str) -> bool:
    q_low = str(query or "").lower()
    return any(alias in q_low for alias in INTENT_ALIASES)


def _extract_customer_query(query: str) -> str | None:
    text = str(query or "").strip()

    explicit = re.search(
        r"\b(?:account(?:\s+prefix)?|customer(?:\s+(?:number|id))?|prefix)\s*[:\-]?\s*([A-Za-z][A-Za-z0-9-]{1,})\b",
        text,
        flags=re.IGNORECASE,
    )
    if explicit and explicit.group(1):
        return explicit.group(1).strip().upper()

    stop_words = {
        "analyze",
        "customer",
        "account",
        "prefix",
        "number",
        "id",
        "for",
        "the",
        "this",
        "run",
        "credits",
        "credit",
    }
    for token in re.findall(r"[A-Za-z][A-Za-z0-9-]*", text):
        value = token.strip()
        if len(value) < 3:
            continue
        if value.lower() in stop_words:
            continue
        return value.upper()

    return None


def _extract_match_mode(query: str) -> str:
    q_low = str(query or "").lower()
    if "customer number" in q_low or "customer id" in q_low:
        return "customer_number"
    if "auto" in q_low:
        return "auto"
    return "account_prefix"


def _build_suggestions(normalized_query: str) -> list[dict[str, str]]:
    return [
        {
            "id": "customer_history",
            "label": f"Show all tickets for customer {normalized_query}",
            "prefix": f"show all tickets for customer {normalized_query}",
        },
    ]


def intent_customer_analysis(query: str, df: pd.DataFrame):
    if not _is_explicit_analyze_query(query):
        return None

    customer_query = _extract_customer_query(query)
    if not customer_query:
        return (
            "Please provide an account prefix or customer number to analyze, for example: `analyze account SGP`.",
            None,
            {},
        )

    match_mode = _extract_match_mode(query)

    try:
        service = get_runtime_service(refresh=False)
        analysis = service.analyze_customer(
            customer_query=customer_query,
            match_mode=match_mode,
            threshold_days=30,
        )
    except Exception as exc:
        return (f"I couldn't run customer analysis for {customer_query}: {exc}", None, {})

    normalized = str(analysis.get("normalized_query") or customer_query).strip()
    text = (
        f"{'Account prefix' if analysis.get('match_mode') != 'customer_number' else 'Customer'} "
        f"{normalized} analysis generated. Review the analysis card below."
    )

    return (
        text,
        None,
        {
            "show_table": False,
            "customer_analysis": dict(analysis),
            "suggestions": _build_suggestions(normalized),
        },
    )
