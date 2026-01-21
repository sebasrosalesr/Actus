import re
import pandas as pd
from datetime import datetime

from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "credit overview",
    "overall overview",
    "summary of credits",
]


def intent_overall_summary(query: str, df: pd.DataFrame) -> str | None:
    """
    Handle high-level questions like:
      - "Actus, give me a credit overview"
      - "SkyBar, summary of open credits"
      - "What's the current credit picture?"
    """
    q_low = query.lower()

    # Must sound like a summary / overview kind of question
    keywords_any = ["summary", "overview", "picture", "status", "how are credits", "credit overview"]
    if not any(k in q_low for k in keywords_any):
        return None

    dv = df.copy()

    # --- Date handling ---
    if "Date" in dv.columns:
        dv["Date"] = coerce_date(dv["Date"])
    else:
        dv["Date"] = pd.NaT

    today = pd.Timestamp.today().normalize()
    this_month_start = today.replace(day=1)

    # --- Base metrics ---
    total_records = len(dv)

    if "Credit Request Total" in dv.columns:
        dv["Credit Request Total_num"] = pd.to_numeric(
            dv["Credit Request Total"], errors="coerce"
        )
        total_amount = dv["Credit Request Total_num"].sum()
        total_amount_str = format_money(total_amount)
    else:
        dv["Credit Request Total_num"] = pd.NA
        total_amount_str = "N/A"

    # --- Open tickets without RTN_CR_No ---
    open_mask = pd.Series([True] * len(dv))
    if "Status" in dv.columns:
        # If you ever add closed markers, you can filter them out here
        pass

    rtn_col = "RTN_CR_No"
    if rtn_col in dv.columns:
        rtn_raw = dv[rtn_col].astype(str).str.strip().str.upper()
        no_rtn_mask = (rtn_raw == "") | rtn_raw.isin(["NAN", "NONE", "NULL"])
    else:
        no_rtn_mask = pd.Series([False] * len(dv))

    open_no_rtn = dv[open_mask & no_rtn_mask].copy()
    open_count = len(open_no_rtn)
    open_amount = open_no_rtn["Credit Request Total_num"].sum(skipna=True)
    open_amount_str = format_money(open_amount) if pd.notna(open_amount) else "N/A"

    # --- This month activity ---
    if "Date" in dv.columns:
        month_mask = dv["Date"].between(this_month_start, today)
        month_df = dv[month_mask].copy()
    else:
        month_df = dv.iloc[0:0].copy()

    month_count = len(month_df)
    month_amount = month_df["Credit Request Total_num"].sum(skipna=True)
    month_amount_str = format_money(month_amount) if pd.notna(month_amount) else "N/A"

    # Top customers this month
    top_lines = []
    if month_count > 0 and "Customer Number" in month_df.columns:
        top_cust = (
            month_df.groupby("Customer Number", dropna=True)["Credit Request Total_num"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
        for cust, amt in top_cust.items():
            top_lines.append(f"- **{cust}** — {format_money(amt)} this month")

    lines = []

    lines.append("📊 **Overall Credit Overview**")
    lines.append("")
    lines.append(f"- Total records in dataset: **{total_records}**")
    lines.append(f"- Total `Credit Request Total`: **{total_amount_str}**")
    lines.append("")
    lines.append("🧾 **Open tickets without a credit number (RTN_CR_No)**")
    lines.append(f"- Count: **{open_count}**")
    lines.append(f"- Sum of `Credit Request Total`: **{open_amount_str}**")
    lines.append("")
    lines.append(
        f"📅 **This month ({this_month_start.date()} → {today.date()})**"
    )
    lines.append(f"- Credit records: **{month_count}**")
    lines.append(f"- Sum of `Credit Request Total`: **{month_amount_str}**")

    if top_lines:
        lines.append("")
        lines.append("🏢 **Top customers by credit this month**")
        lines.extend(top_lines)

    # Optional: most recent 5 credits this month
    if month_count > 0:
        lines.append("")
        lines.append("🕒 **Most recent credits this month (up to 5):**")
        month_sample = (
            month_df.sort_values("Date", ascending=False)
            .head(5)
        )
        for _, r in month_sample.iterrows():
            d = r.get("Date")
            d_str = d.strftime("%Y-%m-%d") if isinstance(d, pd.Timestamp) else "Unknown date"
            tnum = r.get("Ticket Number", "N/A")
            cust = r.get("Customer Number", "N/A")
            amt = r.get("Credit Request Total_num", None)
            amt_str = format_money(amt) if pd.notna(amt) else "N/A"
            lines.append(
                f"- **{d_str}** — Ticket **{tnum}**, Customer **{cust}**, Amount: {amt_str}"
            )

    return "\n".join(lines)
