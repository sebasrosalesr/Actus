"""Deterministic time reasoning utilities for ticket enrichment.

Example:
    from actus.intents._time_reasoning import enrich_time_reasoning, summarize_time_reasoning

    df = enrich_time_reasoning(df)
    summary = summarize_time_reasoning(df)
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from .classify import classify_row
from .features import compute_days_open, compute_days_since_touch, get_col, safe_numeric
from .status_map import macro_phase_from_status
from .summarize import summarize_time_reasoning

__all__ = [
    "enrich_time_reasoning",
    "summarize_time_reasoning",
    "macro_phase_from_status",
]


def enrich_time_reasoning(df: pd.DataFrame, *, now: Optional[datetime] = None) -> pd.DataFrame:
    """Enrich a ticket DataFrame with time reasoning columns.

    Args:
        df: Input DataFrame containing ticket rows.
        now: Optional reference datetime for follow-up checkpoints.

    Returns:
        Copy of df with added enrichment columns.
    """
    now = now or datetime.utcnow()
    enriched = df.copy()

    days_open = compute_days_open(enriched)
    days_since_touch = compute_days_since_touch(enriched)

    credit_col = get_col(enriched, ["Credit_Request_Total", "CREDIT REQUEST TOTAL"])
    if credit_col is None:
        credit_total = pd.Series([0.0] * len(enriched), index=enriched.index, dtype=float)
    else:
        credit_total = safe_numeric(enriched[credit_col])

    status_col = get_col(enriched, ["Last_Status_Message"])
    if status_col is None:
        statuses = pd.Series([None] * len(enriched), index=enriched.index, dtype=object)
    else:
        statuses = enriched[status_col].astype(object)

    phase_noise = statuses.apply(macro_phase_from_status)
    enriched["Macro_Phase"] = phase_noise.apply(lambda item: item[0])
    enriched["Is_System_Noise"] = phase_noise.apply(lambda item: item[1])

    enriched["_days_open"] = days_open
    enriched["_days_since_touch"] = days_since_touch
    enriched["_credit_total"] = credit_total

    classified = enriched.apply(
        lambda row: classify_row(
            row["Macro_Phase"],
            float(row["_days_open"]),
            float(row["_days_since_touch"]),
            float(row["_credit_total"]),
            bool(row["Is_System_Noise"]),
            now=now,
        ),
        axis=1,
        result_type="expand",
    )

    enriched["Follow_Up_Intent"] = classified["follow_up_intent"]
    enriched["Delay_Category"] = classified["delay_category"]
    enriched["Delay_Reason"] = classified["delay_reason"]
    enriched["Delay_Score"] = classified["delay_score"].astype(float)
    enriched["Checkpoint_At"] = classified["checkpoint_at"]

    enriched.drop(columns=["_days_open", "_days_since_touch", "_credit_total"], inplace=True)

    return enriched
