import re
import pandas as pd

from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "top items",
    "top item",
    "items with most credits",
    "top credited items",
]


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


def intent_top_items(query: str, df: pd.DataFrame):
    """
    Handle questions like:
      - "Which items have the most credits issued?"
      - "Show top credited items."
      - "What items are getting the most credits?"
    """
    q = query.lower()

    # Must be about items/products + credits + some notion of "top"
    if not any(w in q for w in ["item", "items", "sku", "product", "products"]):
        return None
    has_credit = "credit" in q or "credits" in q
    has_top = any(w in q for w in ["most", "top", "highest", "biggest"])
    if not has_top:
        return None
    if not has_credit and not any(phrase in q for phrase in ["top items", "top item", "most items"]):
        return None

    dv = df.copy()

    item_col = _find_item_column(dv)
    if item_col is None:
        return (
            "I couldn't identify an item column "
            "(looked for 'Item Number', 'Item', 'Item ID', etc.), "
            "so I can't rank items by credits."
        )

    if "Credit Request Total" not in dv.columns:
        return (
            "I don't see a `Credit Request Total` column, so I can't compute "
            "which items have the most credits."
        )

    dv["Credit Request Total"] = pd.to_numeric(
        dv["Credit Request Total"], errors="coerce"
    )
    dv = dv[dv["Credit Request Total"].notna() & (dv["Credit Request Total"] != 0)]

    # If they say "issued" or explicitly reference credit numbers, filter to RTN_CR_No
    if any(w in q for w in ["issued", "with credit number", "have credit numbers"]):
        if "RTN_CR_No" in dv.columns:
            dv = dv[dv["RTN_CR_No"].fillna("").astype(str).str.strip().ne("")]

    if dv.empty:
        return "I don't see any credit records I can use to rank items."

    # Count unique tickets per item, and total credit dollars
    if "Ticket Number" in dv.columns:
        grouped = (
            dv.groupby(item_col)
            .agg(
                ticket_count=("Ticket Number", "nunique"),
                credit_total=("Credit Request Total", "sum"),
            )
            .reset_index()
        )
    else:
        grouped = (
            dv.groupby(item_col)
            .agg(
                ticket_count=("Credit Request Total", "size"),
                credit_total=("Credit Request Total", "sum"),
            )
            .reset_index()
        )

    grouped = grouped.sort_values(
        ["credit_total", "ticket_count"], ascending=[False, False]
    )

    top_n = grouped.head(10)

    message = "\n".join(
        [
            "Here are the **items with the most credits** "
            "(ranked by total `Credit Request Total`):",
            f"- Total items in ranking: **{len(grouped)}**",
            "",
            "Here is a preview of the results.",
        ]
    )

    preview = top_n.rename(
        columns={
            item_col: "Item",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )

    full_df = grouped.rename(
        columns={
            item_col: "Item",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "top_items.csv",
            "csv_rows": full_df,
            "csv_row_count": len(full_df),
            "columns": [
                "Item",
                "Tickets",
                "Credit Request Total",
            ],
        },
    )
