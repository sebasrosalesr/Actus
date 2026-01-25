import re
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from dateutil import parser

INTENT_ALIASES = [
    "credit amount chart",
    "credit amount plot",
    "plot credit amounts",
]


def _parse_window(query: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str | None]:
    q_low = query.lower()
    tz = ZoneInfo("America/Indiana/Indianapolis")
    now = pd.Timestamp.now(tz=tz)
    today = now.normalize()

    m = re.search(r"\bbetween\s+(.+?)\s+(?:and|to)\s+(.+)", q_low)
    if m:
        raw_start = m.group(1).strip()
        raw_end = m.group(2).strip()
        try:
            dt_start = parser.parse(raw_start, fuzzy=True)
            dt_end = parser.parse(raw_end, fuzzy=True)
            start = pd.Timestamp(dt_start).tz_localize(tz) if dt_start.tzinfo is None else pd.Timestamp(dt_start).tz_convert(tz)
            end = pd.Timestamp(dt_end).tz_localize(tz) if dt_end.tzinfo is None else pd.Timestamp(dt_end).tz_convert(tz)
            return start.normalize(), end, f"between {start.date()} and {end.date()}"
        except Exception:
            pass

    m = re.search(r"\bfrom\s+(.+?)\s+to\s+(.+)", q_low)
    if m:
        raw_start = m.group(1).strip()
        raw_end = m.group(2).strip()
        try:
            dt_start = parser.parse(raw_start, fuzzy=True)
            dt_end = parser.parse(raw_end, fuzzy=True)
            start = pd.Timestamp(dt_start).tz_localize(tz) if dt_start.tzinfo is None else pd.Timestamp(dt_start).tz_convert(tz)
            end = pd.Timestamp(dt_end).tz_localize(tz) if dt_end.tzinfo is None else pd.Timestamp(dt_end).tz_convert(tz)
            return start.normalize(), end, f"from {start.date()} to {end.date()}"
        except Exception:
            pass

    m = re.search(r"\bsince\s+(.+)", q_low)
    if m:
        raw = m.group(1).strip()
        try:
            dt = parser.parse(raw, fuzzy=True)
            start = pd.Timestamp(dt).tz_localize(tz) if dt.tzinfo is None else pd.Timestamp(dt).tz_convert(tz)
            return start.normalize(), now, f"since {start.date()}"
        except Exception:
            pass

    m = re.search(r"\bfrom\s+(.+?)\s+to\s+(.+)", q_low)
    if m:
        raw_start = m.group(1).strip()
        raw_end = m.group(2).strip()
        try:
            dt_start = parser.parse(raw_start, fuzzy=True)
            dt_end = parser.parse(raw_end, fuzzy=True)
            start = pd.Timestamp(dt_start).tz_localize(tz) if dt_start.tzinfo is None else pd.Timestamp(dt_start).tz_convert(tz)
            end = pd.Timestamp(dt_end).tz_localize(tz) if dt_end.tzinfo is None else pd.Timestamp(dt_end).tz_convert(tz)
            return start.normalize(), end, f"from {start.date()} to {end.date()}"
        except Exception:
            pass

    m = re.search(r"\bsince\s+(.+)", q_low)
    if m:
        raw = m.group(1).strip()
        try:
            dt = parser.parse(raw, fuzzy=True)
            start = pd.Timestamp(dt).tz_localize(tz) if dt.tzinfo is None else pd.Timestamp(dt).tz_convert(tz)
            return start.normalize(), now, f"since {start.date()}"
        except Exception:
            pass

    if "today" in q_low:
        return today, now, "today"
    if "yesterday" in q_low:
        start = today - pd.Timedelta(days=1)
        end = today - pd.Timedelta(seconds=1)
        return start, end, "yesterday"

    m = re.search(r"last\s+(\d+)\s+day", q_low)
    if m:
        days = int(m.group(1))
        start = today - pd.Timedelta(days=days)
        return start, now, f"last {days} days"

    m = re.search(r"(?:last|past)\s+(\d+)\s+weeks?", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=7 * weeks)
        return start, now, f"last {weeks} weeks"

    m = re.search(r"(?:last|past)\s+(\d+)\s+month", q_low)
    if m:
        months = int(m.group(1))
        # Use calendar-month start for clearer month buckets (full prior months + current MTD).
        start = (today - pd.DateOffset(months=months)).replace(day=1)
        return start, now, f"last {months} months (from {start.date()} to today)"

    m = re.search(r"(?:from\s+)?(\d+)\s+months?\s+ago", q_low)
    if m:
        months = int(m.group(1))
        start = today - pd.DateOffset(months=months)
        return start, now, f"from {months} months ago"

    m = re.search(r"(?:from\s+)?(\d+)\s+weeks?\b", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=7 * weeks)
        return start, now, f"from {weeks} weeks ago"

    m = re.search(r"last\s+(\d+)\s+week", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=weeks * 7)
        return start, now, f"last {weeks} weeks"

    m = re.search(r"last\s+(\d+)\s+month", q_low)
    if m:
        months = int(m.group(1))
        start = (today - pd.DateOffset(months=months)).replace(day=1)
        return start, now, f"last {months} months (from {start.date()} to today)"

    if "last week" in q_low:
        start = today - pd.Timedelta(days=7)
        return start, now, "last 7 days"

    if "last month" in q_low:
        start = today - pd.Timedelta(days=30)
        return start, now, "last 30 days"

    if "this week" in q_low:
        start = today - pd.Timedelta(days=today.weekday())
        return start, now, "this week"

    if "this month" in q_low:
        start = today.replace(day=1)
        return start, now, "this month"

    if "this year" in q_low:
        start = today.replace(month=1, day=1)
        return start, now, "this year"

    return None, None, None


def intent_credit_amount_plot(query: str, df: pd.DataFrame):
    q_low = query.lower()
    if not any(term in q_low for term in ["plot", "chart", "graph", "trend"]):
        return None
    if "credit" not in q_low:
        return None

    df_use = df.copy()
    df_use["Date"] = pd.to_datetime(df_use.get("Date"), errors="coerce", utc=True)

    start_window, end_window, window_label = _parse_window(query)
    if not (start_window and end_window):
        return (
            "What date range should I use for the credit amount chart? "
            "Try: today, yesterday, last 7 days, last month, or a custom range.",
            None,
            {"follow_up": {"intent": "credit_amount_plot", "prefix": "credit amount chart"}},
        )
    if start_window and end_window:
        start_utc = start_window.tz_convert("UTC")
        end_utc = end_window.tz_convert("UTC")
    else:
        tz = ZoneInfo("America/Indiana/Indianapolis")
        start_window = pd.Timestamp("2025-01-01", tz=tz)
        end_window = pd.Timestamp.now(tz=tz)
        start_utc = start_window.tz_convert("UTC")
        end_utc = end_window.tz_convert("UTC")
        window_label = "from 2025-01-01 to today"

    df_use = df_use[
        df_use["Date"].notna() &
        (df_use["Date"] >= start_utc) &
        (df_use["Date"] <= end_utc)
    ].copy()

    if df_use.empty:
        return f"I don't see any credit records in the window ({window_label})."

    df_use["Credit Request Total"] = pd.to_numeric(
        df_use.get("Credit Request Total"), errors="coerce"
    ).fillna(0)

    cr = df_use.get("RTN_CR_No", pd.Series(index=df_use.index, dtype="object"))
    cr_clean = cr.astype(str).str.strip()
    df_use["Has_CR"] = (~cr.isna()) & (cr_clean != "") & (~cr_clean.str.lower().isin(["nan", "none", "null"]))

    tz = ZoneInfo("America/Indiana/Indianapolis")
    local_dates = df_use["Date"].dt.tz_convert(tz)
    days_span = (local_dates.max().normalize() - local_dates.min().normalize()).days if not local_dates.empty else 0
    use_daily = days_span <= 31

    if use_daily:
        df_use["Bucket"] = local_dates.dt.date
        label = "daily"
    else:
        df_use["Bucket"] = local_dates.dt.to_period("M").dt.to_timestamp()
        label = "monthly"

    grouped = df_use.groupby("Bucket").agg(
        With_CR_USD=("Credit Request Total", lambda x: x[df_use.loc[x.index, "Has_CR"]].sum()),
        Without_CR_USD=("Credit Request Total", lambda x: x[~df_use.loc[x.index, "Has_CR"]].sum()),
        Total_USD=("Credit Request Total", "sum"),
    ).reset_index().sort_values("Bucket")

    grouped["Trend"] = grouped["Total_USD"].rolling(3, min_periods=1).mean()

    data = []
    for _, row in grouped.iterrows():
        bucket = row["Bucket"]
        bucket_label = bucket.strftime("%Y-%m-%d") if hasattr(bucket, "strftime") else str(bucket)
        data.append({
            "bucket": bucket_label,
            "with_cr_usd": float(row["With_CR_USD"]),
            "without_cr_usd": float(row["Without_CR_USD"]),
            "total_usd": float(row["Total_USD"]),
            "trend_usd": float(row["Trend"]),
        })

    message = "\n".join([
        "Credit amount trend:",
        f"- Window used: **{window_label}**",
        f"- Bucketing: **{label}**",
        f"- Records found: **{len(df_use):,}**",
    ])

    return (
        message,
        None,
        {
            "chart": {
                "kind": "credit_amount_trend",
                "bucket": label,
                "window": window_label,
                "data": data,
            }
        },
    )
