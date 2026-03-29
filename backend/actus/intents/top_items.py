from __future__ import annotations

import pandas as pd

from actus.intents._credited_scope import apply_date_window, credited_records_in_window, has_rtn
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "top items",
    "top item",
    "items with most credits",
    "top credited items",
]

_TOP_TERMS = ("most", "top", "highest", "biggest", "driving", "leading")
_CREDITED_TERMS = ("credited", "issued", "credit number", "credit numbers", "rtn", "volume")
_OPEN_TERMS = ("open exposure", "open credit", "open credits", "open liability", "open volume")


def _find_item_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "Item Number",
        "Item",
        "Item ID",
        "Item Code",
        "Item #",
        "ItemNum",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _looks_like_item_ranking_query(query: str) -> bool:
    q = str(query or "").lower()
    if not any(word in q for word in ("item", "items", "sku", "skus", "product", "products")):
        return False
    if not any(word in q for word in _TOP_TERMS):
        return False
    return any(word in q for word in ("credit", "credits", "exposure", "volume", "liability", "credited"))


def _scope_from_query(query: str) -> str:
    q = str(query or "").lower()
    if any(term in q for term in _CREDITED_TERMS):
        return "credited"
    if any(term in q for term in _OPEN_TERMS):
        return "open"
    if "exposure" in q or "liability" in q:
        return "open"
    return "credited"


def _group_items(frame: pd.DataFrame, item_col: str) -> pd.DataFrame:
    if "Ticket Number" in frame.columns:
        grouped = (
            frame.groupby(item_col, dropna=False)
            .agg(
                record_count=("Credit Request Total", "size"),
                ticket_count=("Ticket Number", "nunique"),
                credit_total=("Credit Request Total", "sum"),
            )
            .reset_index()
        )
    else:
        grouped = (
            frame.groupby(item_col, dropna=False)
            .agg(
                record_count=("Credit Request Total", "size"),
                ticket_count=("Credit Request Total", "size"),
                credit_total=("Credit Request Total", "sum"),
            )
            .reset_index()
        )

    return grouped.sort_values(["credit_total", "record_count"], ascending=[False, False]).reset_index(drop=True)


def intent_top_items(query: str, df: pd.DataFrame):
    """
    Handle questions like:
      - "Which items are driving the most open exposure this month?"
      - "Which items have the most credited volume in the last 6 months?"
      - "Show top credited items."
    """
    if not _looks_like_item_ranking_query(query):
        return None

    scope = _scope_from_query(query)
    if scope == "credited":
        credited_rows, rtn_meta, _start, _end, resolved_window = credited_records_in_window(df, query)
        working = credited_rows.copy()
    else:
        working, _start, _end, resolved_window = apply_date_window(df, query)
        rtn_meta = {}
        if "RTN_CR_No" in working.columns:
            working = working[~has_rtn(working["RTN_CR_No"])].copy()

    item_col = _find_item_column(working if not working.empty else df)
    if item_col is None:
        return (
            "I couldn't identify an item column, so I can't rank items by exposure."
        )

    if working.empty:
        label = "credited records" if scope == "credited" else "open exposure"
        return f"I don't see any {label} rows for {resolved_window}."

    if "Credit Request Total" not in working.columns:
        return "I don't see a `Credit Request Total` column, so I can't rank items by volume."

    working["Credit Request Total"] = pd.to_numeric(working["Credit Request Total"], errors="coerce").fillna(0.0)
    working = working[working["Credit Request Total"] != 0].copy()
    if working.empty:
        label = "credited records" if scope == "credited" else "open exposure"
        return f"I don't see any non-zero {label} rows for {resolved_window}."

    grouped = _group_items(working, item_col)
    preview = grouped.head(10).rename(
        columns={
            item_col: "Item",
            "record_count": "Records",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )
    full_df = grouped.rename(
        columns={
            item_col: "Item",
            "record_count": "Records",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )

    if scope == "credited":
        headline = (
            f"Here are the items driving the most **credited volume** in **{resolved_window}**."
        )
    else:
        headline = (
            f"Here are the items driving the most **open exposure** in **{resolved_window}**."
        )

    top_rows = [
        {
            "label": str(row["Item"] or "N/A"),
            "record_count": int(row["Records"]),
            "ticket_count": int(row["Tickets"]),
            "credit_total": float(row["Credit Request Total"] or 0.0),
        }
        for _, row in preview.head(3).iterrows()
    ]

    suggestions = rtn_meta.get("suggestions") if isinstance(rtn_meta, dict) and isinstance(rtn_meta.get("suggestions"), list) else []

    return (
        "\n".join(
            [
                headline,
                f"- Items in ranking: **{len(full_df)}**",
                f"- Total {('credited volume' if scope == 'credited' else 'open exposure')}: **{format_money(full_df['Credit Request Total'].sum())}**",
                "",
                "Here is a preview of the results.",
            ]
        ),
        preview,
        {
            "show_table": True,
            "csv_filename": "top_items.csv",
            "csv_rows": full_df,
            "csv_row_count": len(full_df),
            "columns": ["Item", "Records", "Tickets", "Credit Request Total"],
            "suggestions": suggestions,
            "top_items_summary": {
                "scope": scope,
                "window": resolved_window,
                "data": top_rows,
                "total_credit": float(full_df["Credit Request Total"].sum()),
            },
        },
    )
