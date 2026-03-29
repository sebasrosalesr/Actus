from __future__ import annotations

import pandas as pd

from actus.intents._credited_scope import credited_records_in_window
from actus.intents.credit_ops_snapshot import _lookup_root_causes
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "root cause rtn timing",
    "root causes taking the longest",
    "root causes longest rtn assignment",
]


def _looks_like_root_cause_timing_query(query: str) -> bool:
    q = str(query or "").lower()
    if "root cause" not in q and "root causes" not in q:
        return False
    return any(
        term in q
        for term in (
            "longest",
            "slowest",
            "taking the longest",
            "rtn assignment",
            "days to rtn",
            "time to rtn",
            "reach rtn",
        )
    )


def intent_root_cause_rtn_timing(query: str, df: pd.DataFrame):
    if not _looks_like_root_cause_timing_query(query):
        return None

    credited_rows, _meta, _start, _end, resolved_window = credited_records_in_window(df, query)
    if credited_rows.empty:
        return f"I don't see any credited records with RTN timing data for {resolved_window}."

    working = credited_rows.copy()
    working["Days To RTN Update"] = pd.to_numeric(working.get("Days To RTN Update"), errors="coerce")
    working["Credit Request Total"] = pd.to_numeric(working.get("Credit Request Total"), errors="coerce").fillna(0.0)
    working = working[working["Days To RTN Update"].notna()].copy()
    if working.empty:
        return f"I don't see any RTN timing rows with valid day counts for {resolved_window}."

    causes = _lookup_root_causes(
        working["Ticket Number"],
        working.get("Invoice Number"),
        working.get("Item Number"),
    )
    working["Root Cause"] = (
        causes.get("Root Causes (Primary)", pd.Series(index=working.index, dtype="object"))
        .fillna("Unspecified")
        .astype(str)
        .str.strip()
        .replace({"": "Unspecified"})
    )

    summary = (
        working.groupby("Root Cause", dropna=False)
        .agg(
            record_count=("Root Cause", "size"),
            credit_total=("Credit Request Total", "sum"),
            avg_days_to_rtn=("Days To RTN Update", "mean"),
            median_days_to_rtn=("Days To RTN Update", "median"),
        )
        .sort_values(["avg_days_to_rtn", "record_count", "credit_total"], ascending=[False, False, False])
        .reset_index()
    )

    preview = summary.head(10).rename(
        columns={
            "Root Cause": "Root Cause",
            "record_count": "Records",
            "credit_total": "Credit Request Total",
            "avg_days_to_rtn": "Avg Days To RTN",
            "median_days_to_rtn": "Median Days To RTN",
        }
    )

    top_rows = [
        {
            "root_cause": str(row["Root Cause"] or "Unspecified"),
            "record_count": int(row["Records"]),
            "credit_total": float(row["Credit Request Total"] or 0.0),
            "avg_days_to_rtn": float(row["Avg Days To RTN"] or 0.0),
            "median_days_to_rtn": float(row["Median Days To RTN"] or 0.0),
        }
        for _, row in preview.head(3).iterrows()
    ]

    return (
        "\n".join(
            [
                f"Root causes with the longest time to RTN assignment in **{resolved_window}**:",
                f"- Unique credited records analyzed: **{len(working)}**",
                f"- Longest average RTN timing: **{preview.iloc[0]['Root Cause']}** at **{float(preview.iloc[0]['Avg Days To RTN']):.1f}** day(s)" if not preview.empty else "- No root-cause timing rows were available",
                "",
                "Here is a preview of the results.",
            ]
        ),
        preview,
        {
            "show_table": True,
            "csv_filename": "root_cause_rtn_timing.csv",
            "csv_rows": preview,
            "csv_row_count": len(preview),
            "columns": list(preview.columns),
            "root_cause_rtn_timing": {
                "window": resolved_window,
                "record_count": int(len(working)),
                "data": top_rows,
            },
        },
    )
