"""Summary utilities for time reasoning enrichment."""
from __future__ import annotations

from typing import Dict

import pandas as pd

from .classify import INTENT_I06


def summarize_time_reasoning(df_enriched: pd.DataFrame) -> Dict[str, object]:
    """Summarize enriched time reasoning columns.

    Args:
        df_enriched: DataFrame enriched by enrich_time_reasoning.

    Returns:
        Dict with counts by Macro_Phase, Follow_Up_Intent, Delay_Category, total_rows, num_followups.
    """
    total_rows = int(len(df_enriched))

    if "Macro_Phase" in df_enriched.columns:
        phase_counts = df_enriched["Macro_Phase"].value_counts(dropna=False).to_dict()
    else:
        phase_counts = {}

    if "Follow_Up_Intent" in df_enriched.columns:
        intent_counts = df_enriched["Follow_Up_Intent"].value_counts(dropna=False).to_dict()
        intents = df_enriched["Follow_Up_Intent"].fillna("")
        num_followups = int(((intents != "") & (intents != INTENT_I06)).sum())
    else:
        intent_counts = {}
        num_followups = 0

    if "Delay_Category" in df_enriched.columns:
        category_counts = df_enriched["Delay_Category"].value_counts(dropna=False).to_dict()
    else:
        category_counts = {}

    return {
        "total_rows": total_rows,
        "num_followups": num_followups,
        "macro_phase_counts": phase_counts,
        "follow_up_intent_counts": intent_counts,
        "delay_category_counts": category_counts,
    }


def _self_check() -> None:
    """Run a small manual self-check for enrichment and summary."""
    from . import enrich_time_reasoning

    sample = pd.DataFrame(
        [
            {"Last_Status_Message": "[SYSTEM] Updated on 1/1", "Days_Open": 1, "Days_Since_Last_Status": 1},
            {"Last_Status_Message": "Closed: Completed.", "Days_Open": 12, "Days_Since_Last_Status": 4},
            {
                "Last_Status_Message": "Credit number 123 will close automatically in 14 days",
                "Days_Open": 9,
                "Days_Since_Last_Status": 2,
            },
            {
                "Last_Status_Message": "Submitted to Billing",
                "Days_Open": 20,
                "Days_Since_Last_Status": 12,
                "Credit_Request_Total": 4000,
            },
            {
                "Last_Status_Message": "Pending: Intake",
                "Days_Open": 40,
                "Days_Since_Last_Status": 9,
            },
            {
                "Last_Status_Message": "On hold: awaiting docs",
                "Days_Open": 16,
                "Days_Since_Last_Status": 11,
            },
        ]
    )

    enriched = enrich_time_reasoning(sample)
    print(enriched[["Last_Status_Message", "Macro_Phase", "Follow_Up_Intent", "Delay_Category", "Delay_Score"]])
    print(summarize_time_reasoning(enriched))
