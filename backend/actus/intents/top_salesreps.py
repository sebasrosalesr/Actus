import pandas as pd

INTENT_ALIASES = [
    "top sales reps",
    "sales reps with most credits",
    "top reps",
]


def _find_sales_rep_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "Sales Rep",
        "SalesRep",
        "Sales Representative",
        "Sales Rep Name",
        "Salesperson",
        "Sales Person",
        "Sales Rep ID",
        "Rep",
        "Rep Name",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def intent_top_salesreps(query: str, df: pd.DataFrame):
    """
    Handle questions like:
      - "Which sales reps have the most credits?"
      - "Show top sales reps by credit dollars."
      - "Which reps have the most credits issued?"
    """
    q = query.lower()

    # Must be about sales reps + credits + some notion of "top"
    rep_terms = [
        "sales rep",
        "sales reps",
        "salesrep",
        "sales representative",
        "sales representatives",
        "sales person",
        "salesperson",
    ]
    if not any(term in q for term in rep_terms) and not ("sales" in q and "rep" in q):
        return None
    has_credit = "credit" in q or "credits" in q
    has_top = any(w in q for w in ["most", "top", "highest", "biggest"])
    if not has_top:
        return None
    if not has_credit and not any(phrase in q for phrase in ["top sales reps", "top sales rep", "top reps"]):
        return None

    dv = df.copy()

    # --- find sales rep column ---
    rep_col = _find_sales_rep_column(dv)
    if rep_col is None:
        return (
            "I couldn't identify a sales rep column "
            "(looked for 'Sales Rep', 'Sales Representative', etc.), "
            "so I can't rank sales reps by credits."
        )

    # --- numeric credit total ---
    if "Credit Request Total" not in dv.columns:
        return (
            "I don't see a `Credit Request Total` column, so I can't compute "
            "which sales reps have the most credits."
        )

    dv["Credit Request Total"] = pd.to_numeric(
        dv["Credit Request Total"], errors="coerce"
    )
    dv = dv[dv["Credit Request Total"].notna() & (dv["Credit Request Total"] != 0)]

    # --- If query sounds like "issued" / "have numbers", require RTN_CR_No ---
    if any(w in q for w in ["issued", "with credit number", "have credit numbers"]):
        if "RTN_CR_No" in dv.columns:
            dv = dv[dv["RTN_CR_No"].astype(str).str.strip().ne("")]

    if dv.empty:
        return "I don't see any credit records I can use to rank sales reps."

    # --- group by sales rep ---
    # Count unique tickets if available, otherwise row count
    if "Ticket Number" in dv.columns:
        grouped = (
            dv.groupby(rep_col)
            .agg(
                ticket_count=("Ticket Number", "nunique"),
                credit_total=("Credit Request Total", "sum"),
            )
            .reset_index()
        )
    else:
        grouped = (
            dv.groupby(rep_col)
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
            "Here are the **sales reps with the most credits** "
            "(ranked by total `Credit Request Total`):",
            f"- Total sales reps in ranking: **{len(grouped)}**",
            "",
            "Here is a preview of the results.",
        ]
    )

    preview = top_n.rename(
        columns={
            rep_col: "Sales Rep",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )

    full_df = grouped.rename(
        columns={
            rep_col: "Sales Rep",
            "ticket_count": "Tickets",
            "credit_total": "Credit Request Total",
        }
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "top_salesreps.csv",
            "csv_rows": full_df,
            "csv_row_count": len(full_df),
            "columns": [
                "Sales Rep",
                "Tickets",
                "Credit Request Total",
            ],
        },
    )
