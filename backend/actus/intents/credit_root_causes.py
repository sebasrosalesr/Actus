import pandas as pd

from actus.intents._credited_scope import apply_date_window, has_rtn
from actus.intents.credit_ops_snapshot import _lookup_root_causes
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "root causes",
    "credit root causes",
    "reason for credit",
]


def intent_root_cause_summary(query: str, df: pd.DataFrame):
    """
    Summarize credit totals by root cause (RAG metadata only).
    Example queries:
      - "Summarize total credit amount by root cause"
      - "Credit totals vs root causes"
      - "Show credit amount by reason for credit"
    """
    q_low = query.lower()
    if not (
        "root cause" in q_low
        or "root causes" in q_low
        or "reason for credit" in q_low
        or ("credit" in q_low and "cause" in q_low)
        or ("credit" in q_low and "issue type" in q_low)
    ):
        return None

    if "Credit Request Total" not in df.columns:
        return (
            "I don't see a `Credit Request Total` column, so I can't summarize "
            "credit totals by root cause."
        )

    if "Ticket Number" not in df.columns:
        return (
            "I can't summarize root causes because I don't see a `Ticket Number` "
            "column to look up RAG metadata."
        )

    dv, _start, _end, resolved_window = apply_date_window(df, query)
    if dv.empty:
        return f"I couldn't find any credit rows for {resolved_window}."

    if any(term in q_low for term in ("open exposure", "open credit", "open credits", "open liability", "open")) and "RTN_CR_No" in dv.columns:
        dv = dv[~has_rtn(dv["RTN_CR_No"])].copy()
        if dv.empty:
            return f"I couldn't find any open credit rows for {resolved_window}."

    dv["Credit Request Total"] = pd.to_numeric(
        dv["Credit Request Total"], errors="coerce"
    ).fillna(0.0)

    root_causes = _lookup_root_causes(
        dv["Ticket Number"],
        dv.get("Invoice Number"),
        dv.get("Item Number"),
    )
    cause_series = root_causes["Root Causes (Primary)"]
    has_root = cause_series.notna() & cause_series.astype(str).str.strip().ne("")
    if not has_root.any():
        return (
            "I can't find any root-cause metadata in RAG for the current tickets."
        )

    cause_series = cause_series.astype(str).str.strip()
    cause_series = cause_series.where(has_root, "Unspecified")
    dv["Root Cause"] = cause_series

    summary = (
        dv.groupby("Root Cause", dropna=False)["Credit Request Total"]
        .agg(["sum", "size"])
        .rename(columns={"sum": "Credit Request Total", "size": "Record Count"})
        .reset_index()
        .sort_values("Credit Request Total", ascending=False)
    )

    total_sum = summary["Credit Request Total"].sum()
    summary_top = summary.head(5).copy()

    message = ""
    root_cause_payload = [
        {
            "root_cause": str(row["Root Cause"]),
            "credit_request_total": float(row["Credit Request Total"] or 0),
            "record_count": int(row["Record Count"]),
        }
        for _, row in summary_top.iterrows()
    ]

    return (
        message,
        None,
        {
            "show_table": False,
            "csv_filename": "credit_root_causes.csv",
            "csv_rows": summary_top,
            "csv_row_count": len(summary_top),
            "columns": ["Root Cause", "Credit Request Total", "Record Count"],
            "rootCauses": {
                "period": resolved_window,
                "total": format_money(total_sum),
                "data": root_cause_payload,
            },
        },
    )
