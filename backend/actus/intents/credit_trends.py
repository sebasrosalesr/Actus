import re
import pandas as pd

from actus.intents.credit_ops_snapshot import _parse_window
from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "credit trends",
    "trends in credits",
    "credit patterns",
]


def _naive_day(value: pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def intent_credit_trends(query: str, df: pd.DataFrame):
    """
    Handle queries like:
      - "Are there any trends in credits worth sharing?"
      - "What trends do you see in credits?"
      - "Any recent credit patterns?"

    Compares last 30 days vs previous 30 days:
      - volume (rows)
      - total dollars
      - top customers
      - top items
      - top sales reps
    """

    q_low = query.lower()

    if not (
        "trend" in q_low
        or "pattern" in q_low
        or "insight" in q_low
        or "what's happening" in q_low
        or "whats happening" in q_low
    ):
        return None

    if "credit" not in q_low and "ticket" not in q_low:
        # Let other intents handle non-credit questions
        return None

    if "Date" not in df.columns:
        return "I can't analyze credit trends because the `Date` column is missing."

    dv = df.copy()
    dv["Date"] = coerce_date(dv["Date"])
    dv = dv.dropna(subset=["Date"])

    if dv.empty:
        return "I don't have enough dated records to analyze trends."

    latest = dv["Date"].max().normalize()
    start, end, raw_label = _parse_window(query)
    start_ts = _naive_day(start)
    end_ts = _naive_day(end)

    if start_ts is not None:
        current_start = start_ts
        current_end = end_ts or latest
        if current_end < current_start:
            current_end = current_start
        current_span = current_end - current_start
        prev_end = current_start - pd.Timedelta(days=1)
        prev_start = prev_end - current_span
        period = f"{raw_label or f'{current_start.date()} → {current_end.date()}'} vs previous matched window"
    else:
        # Default behavior: last 30 days vs previous 30 days.
        # If the latest record is mid-month, align the previous window to the
        # end of the prior month so the previous period represents a full month-end.
        if latest.day != latest.days_in_month:
            prev_end = (latest.replace(day=1) - pd.Timedelta(days=1)).normalize()
            prev_start = prev_end - pd.Timedelta(days=30)
            current_start = prev_end + pd.Timedelta(days=1)
            current_end = latest
        else:
            current_end = latest
            current_start = latest - pd.Timedelta(days=30)
            prev_end = current_start - pd.Timedelta(days=1)
            prev_start = prev_end - pd.Timedelta(days=30)
        period = "Last 30 Days vs Previous 30 Days"

    last_30 = dv[dv["Date"].between(current_start, current_end)].copy()
    prev_30 = dv[dv["Date"].between(prev_start, prev_end)].copy()

    if last_30.empty or prev_30.empty:
        return (
            "I don't have enough data in the requested comparison windows to analyze trends."
        )

    # Numeric credit total
    if "Credit Request Total" in dv.columns:
        dv["Credit Request Total"] = pd.to_numeric(
            dv["Credit Request Total"], errors="coerce"
        ).fillna(0.0)
        last_30["Credit Request Total"] = dv.loc[last_30.index, "Credit Request Total"]
        prev_30["Credit Request Total"] = dv.loc[prev_30.index, "Credit Request Total"]
    else:
        last_30["Credit Request Total"] = 0.0
        prev_30["Credit Request Total"] = 0.0

    # ---------- Volume & dollars ----------
    n_last = len(last_30)
    n_prev = len(prev_30)
    diff_n = n_last - n_prev
    pct_n = (diff_n / max(n_prev, 1)) * 100

    amt_last = last_30["Credit Request Total"].sum()
    amt_prev = prev_30["Credit Request Total"].sum()
    diff_amt = amt_last - amt_prev
    pct_amt = (diff_amt / max(amt_prev, 1)) * 100

    # ---------- Top customers (last 30) ----------
    if "Customer Number" in last_30.columns:
        top_cust = (
            last_30.groupby("Customer Number", dropna=False)["Credit Request Total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
    else:
        top_cust = pd.Series(dtype=float)

    # ---------- Top items (last 30) ----------
    if "Item Number" in last_30.columns:
        top_items = (
            last_30.groupby("Item Number", dropna=False)["Credit Request Total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
    else:
        top_items = pd.Series(dtype=float)

    # ---------- Top sales reps (last 30) ----------
    if "Sales Rep" in last_30.columns:
        top_reps = (
            last_30.groupby("Sales Rep", dropna=False)["Credit Request Total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
    else:
        top_reps = pd.Series(dtype=float)

    # ---------- Chart data ----------
    if start_ts is not None:
        chart_start = current_start.replace(day=1)
        chart_end = current_end
    else:
        chart_start = (latest - pd.DateOffset(months=11)).replace(day=1)
        chart_end = latest
    chart_df = dv[dv["Date"].between(chart_start, chart_end)].copy()
    if "RTN_CR_No" in chart_df.columns:
        cr = chart_df["RTN_CR_No"].astype(str).str.strip()
        chart_df["Has_CR"] = (
            chart_df["RTN_CR_No"].notna()
            & (cr != "")
            & (~cr.str.lower().isin(["nan", "none", "null"]))
        )
    else:
        chart_df["Has_CR"] = False

    chart_df["Month"] = chart_df["Date"].dt.to_period("M")
    monthly_totals = (
        chart_df.groupby("Month")["Credit Request Total"]
        .sum()
        .rename("total")
    )
    monthly_with = chart_df.groupby("Month").apply(
        lambda g: g.loc[g["Has_CR"], "Credit Request Total"].sum()
    ).rename("withCr")
    monthly_without = chart_df.groupby("Month").apply(
        lambda g: g.loc[~g["Has_CR"], "Credit Request Total"].sum()
    ).rename("withoutCr")
    monthly = pd.concat([monthly_totals, monthly_with, monthly_without], axis=1)
    monthly = monthly.fillna(0.0).reset_index()
    monthly = monthly.sort_values("Month")
    monthly["trend"] = monthly["total"].rolling(3, min_periods=1).mean()
    monthly["date"] = monthly["Month"].dt.to_timestamp().dt.strftime("%b %Y")
    chart_data = [
        {
            "date": row["date"],
            "withCr": round(float(row["withCr"]), 2),
            "withoutCr": round(float(row["withoutCr"]), 2),
            "trend": round(float(row["trend"]), 2),
        }
        for _, row in monthly.iterrows()
    ]

    credit_trends = {
        "period": period,
        "window": {
            "previous": f"{prev_start.date()} → {prev_end.date()}",
            "current": f"{current_start.date()} → {current_end.date()}",
        },
        "metrics": [
            {
                "label": "Volume (Rows)",
                "current": n_last,
                "previous": n_prev,
                "change": round(pct_n, 1),
                "isCurrency": False,
            },
            {
                "label": "Total Credits",
                "current": round(float(amt_last), 2),
                "previous": round(float(amt_prev), 2),
                "change": round(pct_amt, 1),
                "isCurrency": True,
            },
            {
                "label": "Avg Credit",
                "current": round(float(amt_last / max(n_last, 1)), 2),
                "previous": round(float(amt_prev / max(n_prev, 1)), 2),
                "change": round(
                    ((amt_last / max(n_last, 1)) - (amt_prev / max(n_prev, 1)))
                    / max((amt_prev / max(n_prev, 1)), 1)
                    * 100,
                    1,
                ),
                "isCurrency": True,
            },
        ],
        "topCustomers": [
            {
                "rank": idx,
                "name": str(cust) if pd.notna(cust) else "UNKNOWN",
                "value": format_money(val),
            }
            for idx, (cust, val) in enumerate(top_cust.items(), start=1)
        ],
        "topItems": [
            {
                "rank": idx,
                "name": str(item) if pd.notna(item) else "UNKNOWN",
                "value": format_money(val),
            }
            for idx, (item, val) in enumerate(top_items.items(), start=1)
        ],
        "topReps": [
            {
                "rank": idx,
                "name": str(rep) if pd.notna(rep) else "UNKNOWN",
                "value": format_money(val),
            }
            for idx, (rep, val) in enumerate(top_reps.items(), start=1)
        ],
        "chartData": chart_data,
    }

    message = (
        "Here is the **Credit Trends Analysis** for the requested period.\n\n"
        f"- Previous comparison window: {prev_start.date()} → {prev_end.date()}\n"
        f"- Requested comparison window: {current_start.date()} → {current_end.date()}"
    )

    return message, None, {"creditTrends": credit_trends}
