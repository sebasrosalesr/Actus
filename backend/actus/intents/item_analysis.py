import re
import pandas as pd

from app.rag.new_design.service import get_runtime_service

INTENT_ALIASES = [
    "analyze item",
    "item analysis",
    "run item analysis",
    "analyze this item",
]


def _is_explicit_analyze_query(query: str) -> bool:
    q_low = str(query or "").lower()
    return any(alias in q_low for alias in INTENT_ALIASES)


def _extract_item_number(query: str) -> str | None:
    text = str(query or "").strip()

    explicit = re.search(
        r"\bitem(?:\s*(?:number|#))?\s*[:\-]?\s*([A-Za-z0-9-]{3,})\b",
        text,
        flags=re.IGNORECASE,
    )
    if explicit and explicit.group(1):
        return explicit.group(1).strip().upper()

    # fallback: first token that has a digit and looks like an item id
    for token in re.findall(r"[A-Za-z0-9-]+", text):
        value = token.strip().upper()
        if re.search(r"\d", value) and len(value) >= 5:
            return value

    return None


def intent_item_analysis(query: str, df: pd.DataFrame):
    """
    Handle explicit chat requests like:
      - "analyze item 1007986"
      - "run item analysis for item 016-LP116"
    """
    if not _is_explicit_analyze_query(query):
        return None

    item_number = _extract_item_number(query)
    if not item_number:
        return (
            "Please provide an item number to analyze, for example: `analyze item 1007986`.",
            None,
            {},
        )

    try:
        service = get_runtime_service(refresh=False)
        analysis = service.analyze_item(item_number=item_number)
    except Exception as exc:
        return (f"I couldn't run item analysis for {item_number}: {exc}", None, {})

    text = (
        f"Item {item_number} analysis generated. "
        "Review the analysis card below."
    )

    return (
        text,
        None,
        {
            "show_table": False,
            "item_analysis": dict(analysis),
        },
    )
