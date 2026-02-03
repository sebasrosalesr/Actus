import re
import pandas as pd

from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "top accounts",
    "accounts with most credits",
    "top customers",
]


def _find_customer_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "Customer Number",
        "Customer",
        "Customer Code",
        "Customer ID",
        "Cust #",
        "Cust",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def intent_top_accounts(query: str, df: pd.DataFrame):
    """
    Handle questions like:
      - "Which accounts have the most credits turned in?"
      - "Which customers have the most credits issued?"
      - "Show top accounts by credit dollars."
    """
    q = query.lower()

    # Must be about accounts/customers + some notion of "top"
    if not any(w in q for w in ["account", "accounts", "customer", "customers"]):
        return None
    has_credit = "credit" in q or "credits" in q
    has_top = any(w in q for w in ["most", "top", "highest", "biggest"])
    if not has_top:
        return None
    if not has_credit and not any(phrase in q for phrase in ["top accounts", "top account", "top customers"]):
        return None

    dv = df.copy()

    # --- find customer column ---
    cust_col = _find_customer_column(dv)
    if cust_col is None:
        return (
            "I couldn't identify a customer/account column "
            "(looked for 'Customer', 'Customer Number', etc.), "
            "so I can't rank accounts by credits."
        )

    # --- numeric credit total ---
    if "Credit Request Total" not in dv.columns:
        return (
            "I don't see a `Credit Request Total` column, so I can't compute "
            "which accounts have the most credits."
        )

    dv["Credit Request Total"] = pd.to_numeric(
        dv["Credit Request Total"], errors="coerce"
    )
    dv = dv[dv["Credit Request Total"].notna() & (dv["Credit Request Total"] != 0)]

    # --- If query sounds like "issued" / "have numbers", require RTN_CR_No ---
    if any(w in q for w in ["issued", "with credit number", "have credit numbers"]):
        if "RTN_CR_No" in dv.columns:
            dv = dv[dv["RTN_CR_No"].fillna("").astype(str).str.strip().ne("")]
        # if RTN_CR_No missing, we still continue, just using all credit rows

    if dv.empty:
        return "I don't see any credit records I can use to rank accounts."

    # --- group by customer/account ---
    # Count unique tickets if available, otherwise row count
    if "Ticket Number" in dv.columns:
        grouped = (
            dv.groupby(cust_col)
            .agg(
                ticket_count=("Ticket Number", "nunique"),
                credit_total=("Credit Request Total", "sum"),
            )
            .reset_index()
        )
    else:
        grouped = (
            dv.groupby(cust_col)
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
            "Here are the **accounts/customers with the most credits** "
            "(ranked by total `Credit Request Total`):",
            f"- Total accounts in ranking: **{len(grouped)}**",
            "",
            "Here is a preview of the results.",
        ]
    )

    preview = top_n.rename(
        columns={
            cust_col: "Account",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )

    full_df = grouped.rename(
        columns={
            cust_col: "Account",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "top_accounts.csv",
            "csv_rows": full_df,
            "csv_row_count": len(full_df),
            "columns": [
                "Account",
                "Tickets",
                "Credit Request Total",
            ],
        },
    )
