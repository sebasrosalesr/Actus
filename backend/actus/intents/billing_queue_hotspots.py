from __future__ import annotations

from typing import Any

import pandas as pd

from actus.intents._credited_scope import apply_date_window, has_rtn
from actus.intents._time_reasoning import enrich_time_reasoning
from actus.intents.overall_summary import _latest_status, _latest_status_datetime
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "billing queue delays",
    "billing queue",
    "billing queue hotspots",
]


def _top_groups(frame: pd.DataFrame, column: str, *, limit: int = 5) -> list[dict[str, Any]]:
    if column not in frame.columns or frame.empty:
        return []
    working = frame.copy()
    working[column] = working[column].fillna("").astype(str).str.strip()
    working = working[~working[column].str.upper().isin({"", "N/A", "NA", "NONE", "NULL", "NAN"})].copy()
    if working.empty:
        return []
    grouped = (
        working.groupby(column, dropna=False)
        .agg(
            record_count=("Credit Request Total_num", "size"),
            credit_total=("Credit Request Total_num", "sum"),
        )
        .sort_values(["credit_total", "record_count"], ascending=[False, False])
        .head(limit)
    )
    return [
        {
            "label": str(label or "N/A").strip() or "N/A",
            "record_count": int(row["record_count"]),
            "credit_total": float(row["credit_total"] or 0.0),
        }
        for label, row in grouped.iterrows()
    ]


def intent_billing_queue_hotspots(query: str, df: pd.DataFrame):
    q = str(query or "").lower()
    if "billing queue" not in q:
        return None
    if not any(term in q for term in ("delay", "delays", "accumulating", "where", "stuck", "backlog")):
        return None

    if "Credit Request Total" not in df.columns:
        return "I don't see a `Credit Request Total` column, so I can't summarize billing queue delays."
    if "Status" not in df.columns:
        return "I don't see a `Status` column, so I can't evaluate billing queue delays."

    scoped, _start, _end, resolved_window = apply_date_window(df, query)
    if scoped.empty:
        return f"I couldn't find any credit rows for {resolved_window}."

    scoped["Credit Request Total_num"] = pd.to_numeric(scoped["Credit Request Total"], errors="coerce").fillna(0.0)
    if "RTN_CR_No" in scoped.columns:
        scoped = scoped[~has_rtn(scoped["RTN_CR_No"])].copy()
    if scoped.empty:
        return f"I don't see any open credit rows for {resolved_window}."

    today = pd.Timestamp.today().normalize()
    scoped["Date"] = pd.to_datetime(scoped.get("Date"), errors="coerce")
    scoped["Latest Status"] = scoped["Status"].map(_latest_status)
    scoped["Last Status Date"] = scoped["Status"].map(_latest_status_datetime)
    scoped["Days Open"] = (today - scoped["Date"]).dt.days
    scoped["Days Since Last Status"] = (today - scoped["Last Status Date"]).dt.days
    scoped["Days Since Last Status"] = scoped["Days Since Last Status"].fillna(scoped["Days Open"])
    scoped["Credit_Request_Total"] = scoped["Credit Request Total_num"]
    scoped["Days_Open"] = scoped["Days Open"]
    scoped["Days_Since_Last_Status"] = scoped["Days Since Last Status"]
    scoped["Last_Status_Message"] = scoped["Latest Status"].astype(str)

    enriched = enrich_time_reasoning(scoped)
    delayed = enriched[enriched["Follow_Up_Intent"] == "I04_CHECK_BILLING_QUEUE"].copy()
    if delayed.empty:
        return f"I don't see any billing queue delay rows for {resolved_window}."

    customer_groups = _top_groups(delayed, "Customer Number")
    item_groups = _top_groups(delayed, "Item Number")

    preview = delayed[
        [
            column
            for column in [
                "Ticket Number",
                "Invoice Number",
                "Item Number",
                "Customer Number",
                "Credit Request Total",
                "Days Open",
                "Days Since Last Status",
                "Delay_Reason",
            ]
            if column in delayed.columns
        ]
    ].head(200).copy()

    message_lines = [
        f"Billing queue delay hotspots for **{resolved_window}**:",
        f"- Delayed records: **{len(delayed)}** / **{format_money(delayed['Credit Request Total_num'].sum())}**",
    ]
    if customer_groups:
        message_lines.append("- Top customers:")
        for item in customer_groups[:3]:
            message_lines.append(
                f"  - **{item['label']}** — **{item['record_count']}** record(s) / {format_money(item['credit_total'])}"
            )
    if item_groups:
        message_lines.append("- Top items:")
        for item in item_groups[:3]:
            message_lines.append(
                f"  - **{item['label']}** — **{item['record_count']}** record(s) / {format_money(item['credit_total'])}"
            )
    message_lines.extend(["", "Here is a preview of the delayed rows."])

    return (
        "\n".join(message_lines),
        preview,
        {
            "show_table": True,
            "csv_filename": "billing_queue_hotspots.csv",
            "csv_rows": preview,
            "csv_row_count": len(preview),
            "columns": list(preview.columns),
            "billing_queue_hotspots": {
                "window": resolved_window,
                "record_count": int(len(delayed)),
                "credit_total": float(delayed["Credit Request Total_num"].sum()),
                "top_customers": customer_groups,
                "top_items": item_groups,
            },
        },
    )
