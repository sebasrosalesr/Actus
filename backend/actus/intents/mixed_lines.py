import re
import pandas as pd

INTENT_ALIASES = [
    "mixed lines",
    "mixed line",
    "mixed credits",
]


def _norm(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "", regex=False)
    )


def _extract_ticket_ids(query: str) -> list[str]:
    ids: set[str] = set()
    for m in re.findall(r"\bR-\d{3,}\b", query, flags=re.IGNORECASE):
        ids.add(m.upper())
    return list(ids)


def _has_rtn(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.strip().str.upper()
    return raw.ne("") & ~raw.isin(["NAN", "NONE", "NULL"])


def intent_mixed_lines(query: str, df: pd.DataFrame):
    """
    Handle questions like:
      - "Show mixed lines"
      - "Which tickets have credits with and without CR?"
      - "Show all mixed credit lines"
    """
    q_low = query.lower()

    intent_keywords = [
        "mixed",
        "with and without",
        "without cr",
        "no cr",
        "missing cr",
    ]
    has_with_without = ("with" in q_low and "without" in q_low and ("cr" in q_low or "credit" in q_low))
    if not any(k in q_low for k in intent_keywords) and not has_with_without:
        return None

    if "Ticket Number" not in df.columns:
        return (
            "I can't check mixed lines because I don't see a `Ticket Number` column."
        )

    ticket_ids = _extract_ticket_ids(query)

    ticket_norm = _norm(df["Ticket Number"])
    matched = df.copy()
    if ticket_ids:
        matched = matched[ticket_norm.isin(ticket_ids)].copy()
        if matched.empty:
            return "I couldn't find any records for those ticket numbers."

    if "RTN_CR_No" in matched.columns:
        has_cr = _has_rtn(matched["RTN_CR_No"])
    else:
        has_cr = pd.Series([False] * len(matched), index=matched.index)

    grouped = (
        matched.assign(has_cr=has_cr)
        .groupby("Ticket Number", dropna=False)["has_cr"]
        .agg(has_with="sum", total="size")
        .reset_index()
    )
    grouped["has_without"] = grouped["total"] - grouped["has_with"]
    mixed_ticket_count = int(
        ((grouped["has_with"] > 0) & (grouped["has_without"] > 0)).sum()
    )

    mixed_ticket_numbers = grouped.loc[
        (grouped["has_with"] > 0) & (grouped["has_without"] > 0),
        "Ticket Number",
    ]
    if ticket_ids:
        mixed_mask = matched["Ticket Number"].isin(mixed_ticket_numbers)
        mixed = matched[mixed_mask].copy()
    else:
        mixed = matched[matched["Ticket Number"].isin(mixed_ticket_numbers)].copy()

    if mixed.empty:
        return "I don't see any tickets with mixed lines (with and without CR)."

    if "RTN_CR_No" in mixed.columns:
        mixed_has_cr = _has_rtn(mixed["RTN_CR_No"])
    else:
        mixed_has_cr = pd.Series([False] * len(mixed), index=mixed.index)

    without_cr_count = int((~mixed_has_cr).sum())

    if "Credit Request Total" in mixed.columns:
        mixed["Credit Request Total"] = pd.to_numeric(
            mixed["Credit Request Total"], errors="coerce"
        )
        total_amt = float(mixed["Credit Request Total"].sum(skipna=True))
        with_amt = float(mixed.loc[mixed_has_cr, "Credit Request Total"].sum(skipna=True))
        without_amt = float(mixed.loc[~mixed_has_cr, "Credit Request Total"].sum(skipna=True))
    else:
        total_amt = None
        with_amt = None
        without_amt = None

    # Build per-ticket summary for display and download
    if "Credit Request Total" in mixed.columns:
        mixed["Credit Request Total"] = pd.to_numeric(
            mixed["Credit Request Total"], errors="coerce"
        )
        credit_total = mixed.groupby("Ticket Number")["Credit Request Total"].sum()
    else:
        credit_total = pd.Series(0, index=mixed["Ticket Number"].unique())

    summary = (
        mixed.assign(has_cr=mixed_has_cr)
        .groupby("Ticket Number", dropna=False)
        .agg(
            with_cr=("has_cr", "sum"),
            without_cr=("has_cr", lambda s: (~s).sum()),
        )
        .reset_index()
    )
    summary["Credit Request Total"] = summary["Ticket Number"].map(credit_total)
    summary = summary.sort_values(
        ["Credit Request Total", "with_cr", "without_cr"], ascending=[False, False, False]
    )

    preferred_cols = [
        "Ticket Number",
        "With CR",
        "Without CR",
        "Credit Request Total",
    ]
    summary = summary.rename(
        columns={
            "Ticket Number": "Ticket Number",
            "with_cr": "With CR",
            "without_cr": "Without CR",
        }
    )
    summary = summary[preferred_cols]

    preview = summary.head(30)

    message = (
        "Here are the **mixed credit lines** (tickets with both CR and no-CR records), "
        "including records with and without CR numbers."
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "mixed_lines.csv",
            "csv_rows": summary,
            "csv_row_count": len(summary),
            "columns": preferred_cols,
            "mixedLinesSummary": {
                "mixedTicketCount": mixed_ticket_count,
                "withoutCrCount": without_cr_count,
                "totalUsd": total_amt,
                "withCrUsd": with_amt,
                "withoutCrUsd": without_amt,
            },
        },
    )
