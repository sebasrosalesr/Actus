import pandas as pd
import re
from zoneinfo import ZoneInfo
from dateutil import parser

from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "credit activity",
    "credits updated",
    "credit updates",
    "credits I updated",
]


def _parse_window(query: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str | None]:
    q_low = query.lower()
    tz = ZoneInfo("America/Indiana/Indianapolis")
    now = pd.Timestamp.now(tz=tz)
    today = now.normalize()
    word_to_weeks = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }
    word_to_days = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }

    m = re.search(r"from\s+(.+?)\s+to\s+today", q_low)
    if m:
        raw = m.group(1).strip()
        try:
            dt = parser.parse(raw, fuzzy=True)
            start = pd.Timestamp(dt).tz_localize(tz) if dt.tzinfo is None else pd.Timestamp(dt).tz_convert(tz)
            return start.normalize(), now, f"from {start.date()} to today (Indianapolis)"
        except Exception:
            pass

    m = re.search(r"from\s+(.+?)\s+to\s+(.+)", q_low)
    if m:
        raw_start = m.group(1).strip()
        raw_end = m.group(2).strip()
        try:
            dt_start = parser.parse(raw_start, fuzzy=True)
            dt_end = parser.parse(raw_end, fuzzy=True)
            start = pd.Timestamp(dt_start).tz_localize(tz) if dt_start.tzinfo is None else pd.Timestamp(dt_start).tz_convert(tz)
            end = pd.Timestamp(dt_end).tz_localize(tz) if dt_end.tzinfo is None else pd.Timestamp(dt_end).tz_convert(tz)
            return start.normalize(), end, f"from {start.date()} to {end.date()} (Indianapolis)"
        except Exception:
            pass

    m = re.search(r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?\b", q_low)
    if m:
        raw = m.group(0).strip()
        try:
            dt = parser.parse(raw, fuzzy=True)
            start = pd.Timestamp(dt).tz_localize(tz) if dt.tzinfo is None else pd.Timestamp(dt).tz_convert(tz)
            end = start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            return start.normalize(), end, f"on {start.date()} (Indianapolis)"
        except Exception:
            pass

    if "today" in q_low:
        return today, now, "today (Indianapolis)"
    if "yesterday" in q_low:
        start = today - pd.Timedelta(days=1)
        end = today - pd.Timedelta(seconds=1)
        return start, end, "yesterday (Indianapolis)"

    m = re.search(r"last\s+(\d+)\s+day", q_low)
    if m:
        days = int(m.group(1))
        start = today - pd.Timedelta(days=days)
        return start, now, f"last {days} days (Indianapolis)"

    m = re.search(r"(?:last|past)\s+(\d+)\s+week", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=7 * weeks)
        return start, now, f"last {weeks} weeks (Indianapolis)"

    m = re.search(r"(?:last|past)\s+(\d+)\s+month", q_low)
    if m:
        months = int(m.group(1))
        start = today - pd.DateOffset(months=months)
        return start, now, f"last {months} months (Indianapolis)"

    m = re.search(r"(?:from\s+)?(\d+)\s+months?\s+ago", q_low)
    if m:
        months = int(m.group(1))
        start = today - pd.DateOffset(months=months)
        return start, now, f"from {months} months ago (Indianapolis)"

    m = re.search(r"(?:from\s+)?(\d+)\s+days?\s+ago", q_low)
    if m:
        days = int(m.group(1))
        start = today - pd.Timedelta(days=days)
        return start, now, f"{days} days ago (Indianapolis)"

    m = re.search(r"(?:from\s+)?(one|two|three|four|five|six|seven|eight|nine|ten)\s+days?\s+ago", q_low)
    if m:
        days = word_to_days.get(m.group(1), 1)
        start = today - pd.Timedelta(days=days)
        return start, now, f"{days} days ago (Indianapolis)"

    m = re.search(r"(?:from\s+)?(\d+)\s+weeks?(?:\s+ago)?", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=7 * weeks)
        label = "from {} weeks ago".format(weeks) if "ago" in m.group(0) else f"from {weeks} weeks"
        return start, now, f"{label} (Indianapolis)"

    m = re.search(r"(?:from\s+)?(one|two|three|four|five|six|seven|eight|nine|ten)\s+weeks?(?:\s+ago)?", q_low)
    if m:
        weeks = word_to_weeks.get(m.group(1), 1)
        start = today - pd.Timedelta(days=7 * weeks)
        label = "from {} weeks ago".format(weeks) if "ago" in m.group(0) else f"from {weeks} weeks"
        return start, now, f"{label} (Indianapolis)"

    if "last week" in q_low:
        start = today - pd.Timedelta(days=7)
        return start, now, "last 7 days (Indianapolis)"

    if "last month" in q_low:
        start = today - pd.Timedelta(days=30)
        return start, now, "last 30 days (Indianapolis)"

    if "this week" in q_low:
        start = today - pd.Timedelta(days=today.weekday())
        return start, now, "this week (Indianapolis)"

    if "this month" in q_low:
        start = today.replace(day=1)
        return start, now, "this month (Indianapolis)"

    return None, None, None


def intent_credit_activity(query: str, df: pd.DataFrame):
    """
    Filter credit activity using a 2025 window and last status timestamps.
    Returns a preview table plus CSV export of full filtered records.
    """
    q_low = query.lower()
    if ("credit" not in q_low and "credits" not in q_low) and "credit activity" not in q_low:
        return None
    if ("update" not in q_low and "updated" not in q_low) and "credit activity" not in q_low:
        return None

    df_use = df.copy()
    df_use["Date"] = pd.to_datetime(df_use.get("Date"), errors="coerce", utc=True)

    start_2025 = pd.Timestamp("2025-01-01", tz="UTC")
    end_now = pd.Timestamp.now(tz="UTC")

    df_use = df_use[
        df_use["Date"].notna() &
        (df_use["Date"] >= start_2025) &
        (df_use["Date"] <= end_now)
    ].copy()

    status_series = (
        df_use.get("Status", pd.Series(index=df_use.index, dtype="object"))
        .fillna("")
        .astype(str)
    )

    ts_regex = r"\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]"
    df_use["Last_Status_Time_Str"] = status_series.str.findall(ts_regex).apply(
        lambda xs: xs[-1] if xs else pd.NA
    )
    df_use["Last_Status_Time"] = pd.to_datetime(
        df_use["Last_Status_Time_Str"], errors="coerce", utc=True
    )

    parts = status_series.str.split(ts_regex, regex=True)
    df_use["Last_Status_Message"] = parts.apply(
        lambda p: str(p[-1]).strip() if isinstance(p, list) and len(p) else ""
    )

    start_window, end_window, window_label = _parse_window(query)
    if not start_window or not end_window:
        return (
            "Which time window should I use for credit activity? "
            "Try: today, yesterday, last 7 days, last month, or a custom range like "
            "`from 2 weeks`, `5 days ago`, or `from Jan 10 to Jan 20`.",
            None,
            {"follow_up": {"intent": "credit_activity", "prefix": "credit activity"}},
        )
    if start_window and end_window:
        start_utc = start_window.tz_convert("UTC")
        end_utc = end_window.tz_convert("UTC")
        df_recent_status = df_use[
            df_use["Last_Status_Time"].notna() &
            (df_use["Last_Status_Time"] >= start_utc) &
            (df_use["Last_Status_Time"] <= end_utc)
        ].copy()
    if df_recent_status.empty:
        window_note = f" in the window ({window_label})" if window_label else ""
        return (
            f"I don't see any credit updates with Last_Status_Time on or after "
            f"**{start_2025.date()}** in 2025{window_note}."
        )

    total_records = len(df_recent_status)
    total_credits = pd.to_numeric(
        df_recent_status.get("Credit Request Total"), errors="coerce"
    ).sum()
    total_credits_str = format_money(total_credits)

    if window_label:
        window_line = f"- Window used: **{window_label}**"
        cutoff_line = None
    else:
        window_line = "- Window used: **2025 to date**"
        cutoff_line = f"- Rows with Last_Status_Time >= {start_2025.date()}: **{total_records:,}**"
    window_label_text = window_label or "2025–present"
    message = "\n".join([
        "Credit Activity Snapshot (2025–Present)",
        f"- Total records (2025–present): **{len(df_use):,}**",
        f"- Records in analysis window: **{total_records:,}**",
        f"- Analysis window: **{window_label_text}**",
        f"- Total credit value (window): **{total_credits_str}**",
        "",
        "Here is a preview of the results.",
    ])

    out = (
        df_recent_status[[
            "Date",
            "Ticket Number",
            "Item Number",
            "Invoice Number",
            "Customer Number",
            "RTN_CR_No",
            "Last_Status_Time",
            "Last_Status_Message",
            "Status",
        ]]
        .sort_values("Last_Status_Time", ascending=False)
        .reset_index(drop=True)
    )
    # Present date-only values without timezone shifting in the UI preview/export.
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")

    preview = out.head(200).copy()

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "credit_activity_snapshot.csv",
            "csv_rows": out,
            "csv_row_count": len(out),
            "columns": [
            "Date",
            "Ticket Number",
            "Item Number",
            "Invoice Number",
            "Customer Number",
            "RTN_CR_No",
            "Last_Status_Time",
            "Last_Status_Message",
                "Status",
            ],
        },
    )
