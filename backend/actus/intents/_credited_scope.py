from __future__ import annotations

from typing import Any

import pandas as pd

from actus.intents.credit_ops_snapshot import _parse_window
from actus.intents.system_updates import intent_system_updates


def has_rtn(series: pd.Series) -> pd.Series:
    values = series.fillna("").astype(str).str.strip().str.upper()
    return ~values.isin({"", "NAN", "NONE", "NULL", "NA"})


def naive_ts(value: pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        return ts.tz_localize(None)
    return ts


def window_label(start: pd.Timestamp | None, end: pd.Timestamp | None, fallback: str | None = None) -> str:
    start_ts = naive_ts(start)
    end_ts = naive_ts(end)
    if start_ts is None:
        return fallback or "current dataset"
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    return f"{start_ts.date()} → {end_ts.date()}"


def apply_date_window(
    df: pd.DataFrame,
    query: str,
    *,
    date_col: str = "Date",
) -> tuple[pd.DataFrame, pd.Timestamp | None, pd.Timestamp | None, str]:
    start, end, raw_label = _parse_window(query)
    start_ts = naive_ts(start)
    end_ts = naive_ts(end)
    scoped = df.copy()
    if date_col in scoped.columns:
        scoped[date_col] = pd.to_datetime(scoped[date_col], errors="coerce")
        if start_ts is not None and end_ts is not None:
            scoped = scoped[scoped[date_col].between(start_ts, end_ts)].copy()
        elif start_ts is not None:
            scoped = scoped[scoped[date_col] >= start_ts].copy()
    return scoped, start_ts, end_ts, window_label(start_ts, end_ts, raw_label)


def build_system_updates_query(start: pd.Timestamp | None, end: pd.Timestamp | None) -> str:
    start_ts = naive_ts(start)
    end_ts = naive_ts(end)
    if start_ts is None:
        return "system rtn updates analysis"
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    return (
        "system rtn updates analysis from "
        f"{start_ts.strftime('%Y-%m-%d')} to {end_ts.strftime('%Y-%m-%d')}"
    )


def dedupe_latest_credited_records(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    latest = events.copy()
    latest["Update Event Time"] = pd.to_datetime(latest.get("Update Event Time"), errors="coerce")
    latest["Credit Request Total"] = pd.to_numeric(latest.get("Credit Request Total"), errors="coerce").fillna(0.0)
    dedupe_subset = [
        column
        for column in [
            "Ticket Number",
            "Invoice Number",
            "Item Number",
            "Customer Number",
            "RTN_CR_No",
        ]
        if column in latest.columns
    ]
    if dedupe_subset:
        latest = (
            latest.sort_values("Update Event Time", ascending=True)
            .drop_duplicates(subset=dedupe_subset, keep="last")
            .copy()
        )
    return latest


def credited_records_in_window(
    df: pd.DataFrame,
    query: str,
) -> tuple[pd.DataFrame, dict[str, Any], pd.Timestamp | None, pd.Timestamp | None, str]:
    start, end, raw_label = _parse_window(query)
    start_ts = naive_ts(start)
    end_ts = naive_ts(end)
    system_query = build_system_updates_query(start_ts, end_ts)
    response = intent_system_updates(system_query, df)
    if not isinstance(response, tuple) or len(response) != 3:
        return pd.DataFrame(), {}, start_ts, end_ts, window_label(start_ts, end_ts, raw_label)

    _text, _rows, meta = response
    if not isinstance(meta, dict):
        return pd.DataFrame(), {}, start_ts, end_ts, window_label(start_ts, end_ts, raw_label)

    csv_rows = meta.get("csv_rows")
    if not isinstance(csv_rows, pd.DataFrame) or csv_rows.empty:
        return pd.DataFrame(), meta, start_ts, end_ts, window_label(start_ts, end_ts, raw_label)

    return (
        dedupe_latest_credited_records(csv_rows),
        meta,
        start_ts,
        end_ts,
        window_label(start_ts, end_ts, raw_label),
    )
