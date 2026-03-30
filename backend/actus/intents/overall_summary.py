import re
from typing import Any

import pandas as pd

from actus.intents._time_reasoning import enrich_time_reasoning
from actus.intents.credit_ops_snapshot import _lookup_root_causes, _parse_window
from actus.intents.system_updates import intent_system_updates
from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "credit overview",
    "overall overview",
    "summary of credits",
]


def _empty_time_metrics() -> dict[str, Any]:
    return {
        "avg_days_open": 0.0,
        "avg_days_since_last_status": 0.0,
        "billing_queue_delay_count": 0,
        "billing_queue_delay_total": 0.0,
        "stale_investigation_count": 0,
        "stale_investigation_total": 0.0,
    }


def _empty_credited_metrics() -> dict[str, Any]:
    return {
        "credited_record_count": 0,
        "credited_credit_total": 0.0,
        "credited_event_count": 0,
        "credited_event_credit_total": 0.0,
        "primary_system_record_count": 0,
        "primary_system_credit_total": 0.0,
        "primary_manual_record_count": 0,
        "primary_manual_credit_total": 0.0,
        "system_record_count": 0,
        "system_credit_total": 0.0,
        "manual_record_count": 0,
        "manual_credit_total": 0.0,
        "records_with_both_sources": 0,
        "reopened_after_terminal_count": 0,
        "avg_days_to_rtn_assignment": 0.0,
        "largest_system_batch_count": 0,
        "largest_system_batch_date": "N/A",
        "largest_system_batch_credit_total": 0.0,
        "largest_manual_batch_count": 0,
        "largest_manual_batch_date": "N/A",
        "largest_manual_batch_credit_total": 0.0,
    }


def _has_rtn(series: pd.Series) -> pd.Series:
    values = series.fillna("").astype(str).str.strip().str.upper()
    return ~values.isin({"", "NAN", "NONE", "NULL", "NA"})


def _latest_status(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return "N/A"
    matches = list(
        re.finditer(r"(?:\[)?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]?", text)
    )
    if matches:
        start = matches[-1].start()
        return text[start:].strip()
    return text


def _latest_status_datetime(value: object) -> pd.Timestamp:
    text = str(value or "").strip()
    if not text:
        return pd.NaT
    matches = list(
        re.finditer(r"(?:\[)?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]?", text)
    )
    if not matches:
        return pd.NaT
    return pd.to_datetime(matches[-1].group(1), errors="coerce")


def _top_amount_groups(frame: pd.DataFrame, column: str, *, limit: int = 3) -> list[dict[str, Any]]:
    if column not in frame.columns or frame.empty:
        return []
    grouped = (
        frame.groupby(column, dropna=False)["Credit Request Total_num"]
        .sum()
        .sort_values(ascending=False)
        .head(limit)
    )
    out: list[dict[str, Any]] = []
    for key, amount in grouped.items():
        label = str(key or "N/A").strip() or "N/A"
        out.append(
            {
                "label": label,
                "credit_total": float(amount or 0.0),
            }
        )
    return out


def _root_cause_summary(frame: pd.DataFrame, *, limit: int = 3) -> list[dict[str, Any]]:
    if frame.empty or "Ticket Number" not in frame.columns:
        return []

    causes = _lookup_root_causes(
        frame["Ticket Number"],
        frame.get("Invoice Number"),
        frame.get("Item Number"),
    )
    working = frame.copy()
    working["Root Cause"] = (
        causes.get("Root Causes (Primary)", pd.Series(index=working.index, dtype="object"))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    working = working[working["Root Cause"].ne("")]
    if working.empty:
        return []

    grouped = (
        working.groupby("Root Cause", dropna=False)
        .agg(
            record_count=("Root Cause", "size"),
            credit_total=("Credit Request Total_num", "sum"),
        )
        .sort_values(["record_count", "credit_total"], ascending=[False, False])
        .head(limit)
    )
    out: list[dict[str, Any]] = []
    for root_cause, row in grouped.iterrows():
        out.append(
            {
                "root_cause": str(root_cause),
                "record_count": int(row["record_count"]),
                "credit_total": float(row["credit_total"] or 0.0),
            }
        )
    return out


def _cached_root_cause_summary(frame: pd.DataFrame, *, limit: int = 3) -> list[dict[str, Any]]:
    cache = frame.attrs.get("_actus_intent_cache") if isinstance(getattr(frame, "attrs", None), dict) else None
    if not isinstance(cache, dict):
        return []
    for (intent_id, _query), cached_response in cache.items():
        if intent_id != "credit_root_causes":
            continue
        if not isinstance(cached_response, tuple) or len(cached_response) != 3:
            continue
        _text, _rows, meta = cached_response
        if not isinstance(meta, dict):
            continue
        payload = meta.get("rootCauses")
        if not isinstance(payload, dict):
            continue
        data = payload.get("data")
        if not isinstance(data, list):
            continue
        summary: list[dict[str, Any]] = []
        for item in data[:limit]:
            if not isinstance(item, dict):
                continue
            summary.append(
                {
                    "root_cause": str(item.get("root_cause") or "").strip(),
                    "record_count": int(float(item.get("record_count") or 0)),
                    "credit_total": float(item.get("credit_request_total") or 0.0),
                }
            )
        if summary:
            return summary
    return []


def _time_reasoning_metrics(open_df: pd.DataFrame) -> dict[str, Any]:
    if open_df.empty:
        return _empty_time_metrics()

    enriched = open_df.copy()
    today = pd.Timestamp.today().normalize()
    enriched["Latest Status"] = enriched.get("Status", pd.Series(index=enriched.index, dtype="object")).map(_latest_status)
    enriched["Last Status Date"] = enriched.get("Status", pd.Series(index=enriched.index, dtype="object")).map(_latest_status_datetime)
    enriched["Days Open"] = (today - enriched["Date"]).dt.days
    enriched["Days Since Last Status"] = (today - enriched["Last Status Date"]).dt.days
    enriched["Days Since Last Status"] = enriched["Days Since Last Status"].fillna(enriched["Days Open"])
    enriched["Credit_Request_Total"] = enriched["Credit Request Total_num"]
    enriched["Days_Open"] = enriched["Days Open"]
    enriched["Days_Since_Last_Status"] = enriched["Days Since Last Status"]
    enriched["Last_Status_Message"] = enriched["Latest Status"].astype(str)

    enriched = enrich_time_reasoning(enriched)

    def _intent_totals(intent_id: str) -> tuple[int, float]:
        subset = enriched[enriched["Follow_Up_Intent"] == intent_id]
        return int(len(subset.index)), float(subset["Credit Request Total_num"].sum())

    billing_count, billing_total = _intent_totals("I04_CHECK_BILLING_QUEUE")
    stale_count, stale_total = _intent_totals("I03_ESCALATE_STALE_INVESTIGATION")

    return {
        "avg_days_open": float(enriched["Days Open"].mean() or 0.0),
        "avg_days_since_last_status": float(enriched["Days Since Last Status"].mean() or 0.0),
        "billing_queue_delay_count": billing_count,
        "billing_queue_delay_total": billing_total,
        "stale_investigation_count": stale_count,
        "stale_investigation_total": stale_total,
    }


def _naive_ts(value: pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        return ts.tz_localize(None)
    return ts


def _window_range_label(start: pd.Timestamp | None, end: pd.Timestamp | None) -> str | None:
    start_ts = _naive_ts(start)
    end_ts = _naive_ts(end)
    if start_ts is None:
        return None
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    return f"{start_ts.date()} → {end_ts.date()}"


def _ops_snapshot_suggestion(start: pd.Timestamp | None, end: pd.Timestamp | None) -> dict[str, str] | None:
    start_ts = _naive_ts(start)
    end_ts = _naive_ts(end)
    if start_ts is None:
        return None
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    start_text = start_ts.strftime("%Y-%m-%d")
    end_text = end_ts.strftime("%Y-%m-%d")
    window = f"{start_text} → {end_text}"
    return {
        "id": "credit_ops_snapshot",
        "label": f"Credit ops snapshot ({window})",
        "prefix": f"credit ops snapshot from {start_text} to {end_text}",
    }


def _credit_amount_plot_suggestion(
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> dict[str, str]:
    start_ts = _naive_ts(start)
    end_ts = _naive_ts(end)
    if start_ts is None:
        return {
            "id": "credit_amount_plot",
            "label": "Credit amount chart",
            "prefix": "credit amount chart",
        }
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    start_text = start_ts.strftime("%Y-%m-%d")
    end_text = end_ts.strftime("%Y-%m-%d")
    window = f"{start_text} → {end_text}"
    return {
        "id": "credit_amount_plot",
        "label": f"Credit amount chart ({window})",
        "prefix": f"credit amount chart from {start_text} to {end_text}",
    }


def _rtn_updates_query(start: pd.Timestamp | None, end: pd.Timestamp | None) -> str:
    start_ts = _naive_ts(start)
    end_ts = _naive_ts(end)
    if start_ts is None:
        return "system rtn updates analysis"
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    return (
        "system rtn updates analysis from "
        f"{start_ts.strftime('%Y-%m-%d')} to {end_ts.strftime('%Y-%m-%d')}"
    )


def _credited_in_period_metrics(
    df: pd.DataFrame,
    *,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    empty_payload = _empty_credited_metrics()

    rtn_query = _rtn_updates_query(start, end)
    response = None
    cache = df.attrs.get("_actus_intent_cache") if isinstance(getattr(df, "attrs", None), dict) else None
    if isinstance(cache, dict):
        cached_response = cache.get(("system_updates", rtn_query))
        if isinstance(cached_response, tuple) and len(cached_response) == 3:
            response = cached_response

    if response is None:
        response = intent_system_updates(rtn_query, df)
    if not isinstance(response, tuple) or len(response) != 3:
        return empty_payload, []

    _text, _rows, meta = response
    if not isinstance(meta, dict):
        return empty_payload, []

    summary = meta.get("system_updates_summary")
    csv_rows = meta.get("csv_rows")
    if not isinstance(summary, dict) or not isinstance(csv_rows, pd.DataFrame) or csv_rows.empty:
        return empty_payload, meta.get("suggestions") if isinstance(meta.get("suggestions"), list) else []

    events = csv_rows.copy()
    events["Update Event Time"] = pd.to_datetime(events.get("Update Event Time"), errors="coerce")
    events["Credit Request Total"] = pd.to_numeric(events.get("Credit Request Total"), errors="coerce").fillna(0.0)
    events["Days To RTN Update"] = pd.to_numeric(events.get("Days To RTN Update"), errors="coerce")
    events["Update Source"] = events.get("Update Source", pd.Series(index=events.index, dtype="object")).fillna("").astype(str)
    events["Primary Update Source"] = events.get("Primary Update Source", pd.Series(index=events.index, dtype="object")).fillna("").astype(str)
    events["Reopened After Terminal"] = events.get("Reopened After Terminal", pd.Series(index=events.index, dtype="object")).fillna(False).astype(bool)

    dedupe_subset = [
        column
        for column in [
            "Ticket Number",
            "Invoice Number",
            "Item Number",
            "Customer Number",
            "RTN_CR_No",
        ]
        if column in events.columns
    ]
    record_key = (
        events[dedupe_subset].apply(
            lambda row: "||".join("" if pd.isna(value) else str(value) for value in row),
            axis=1,
        )
        if dedupe_subset
        else pd.Series(events.index.astype(str), index=events.index, dtype="object")
    )
    events["_Record_Key"] = record_key
    if dedupe_subset:
        latest = (
            events.sort_values("Update Event Time", ascending=True)
            .drop_duplicates(subset=dedupe_subset, keep="last")
            .copy()
        )
    else:
        latest = events.copy()

    valid_days = pd.to_numeric(events["Days To RTN Update"], errors="coerce").dropna()
    records_with_both_sources = 0
    if dedupe_subset:
        source_counts = events.groupby("_Record_Key", dropna=False)["Update Source"].nunique()
        records_with_both_sources = int((source_counts > 1).sum())

    grouped = events.sort_values("Update Event Time", ascending=True).groupby("_Record_Key", dropna=False)

    def _primary_row(group: pd.DataFrame) -> pd.Series:
        primary_source = str(group["Primary Update Source"].iloc[-1] or "").strip().lower()
        preferred = group[group["Update Source"].astype(str).str.strip().str.lower().eq(primary_source)].copy()
        if preferred.empty:
            preferred = group.copy()
        return preferred.sort_values("Update Event Time", ascending=True).iloc[0]

    primary_rows = grouped.apply(_primary_row).reset_index(drop=True) if not events.empty else pd.DataFrame()
    primary_rows["Credit Request Total"] = pd.to_numeric(primary_rows.get("Credit Request Total"), errors="coerce").fillna(0.0)
    primary_rows["Days To RTN Update"] = pd.to_numeric(primary_rows.get("Days To RTN Update"), errors="coerce")
    primary_rows["Primary Update Source"] = primary_rows.get("Primary Update Source", pd.Series(index=primary_rows.index, dtype="object")).fillna("").astype(str)
    primary_rows["Reopened After Terminal"] = primary_rows.get("Reopened After Terminal", pd.Series(index=primary_rows.index, dtype="object")).fillna(False).astype(bool)

    primary_system = primary_rows[primary_rows["Primary Update Source"].str.lower().eq("system")].copy()
    primary_manual = primary_rows[primary_rows["Primary Update Source"].str.lower().eq("manual")].copy()
    primary_days = pd.to_numeric(primary_rows["Days To RTN Update"], errors="coerce").dropna()
    reopened_after_terminal_count = int(primary_rows["Reopened After Terminal"].fillna(False).astype(bool).sum()) if not primary_rows.empty else 0

    payload = {
        "credited_record_count": int(len(latest.index)),
        "credited_credit_total": float(latest["Credit Request Total"].sum()),
        "credited_event_count": int(summary.get("preview_total_records") or len(events.index)),
        "credited_event_credit_total": float(events["Credit Request Total"].sum()),
        "primary_system_record_count": int(len(primary_system.index)),
        "primary_system_credit_total": float(primary_system["Credit Request Total"].sum()),
        "primary_manual_record_count": int(len(primary_manual.index)),
        "primary_manual_credit_total": float(primary_manual["Credit Request Total"].sum()),
        "system_record_count": int(summary.get("total_records") or 0),
        "system_credit_total": float(summary.get("credit_total") or 0.0),
        "manual_record_count": int(summary.get("manual_record_count") or 0),
        "manual_credit_total": float(summary.get("manual_credit_total") or 0.0),
        "records_with_both_sources": records_with_both_sources,
        "reopened_after_terminal_count": reopened_after_terminal_count,
        "avg_days_to_rtn_assignment": float(primary_days.mean() or 0.0) if not primary_days.empty else 0.0,
        "largest_system_batch_count": int(summary.get("largest_batch_count") or 0),
        "largest_system_batch_date": str(summary.get("largest_batch_date") or "N/A"),
        "largest_system_batch_credit_total": float(summary.get("largest_batch_credit_total") or 0.0),
        "largest_manual_batch_count": int(summary.get("manual_largest_batch_count") or 0),
        "largest_manual_batch_date": str(summary.get("manual_largest_batch_date") or "N/A"),
        "largest_manual_batch_credit_total": float(summary.get("manual_largest_batch_credit_total") or 0.0),
    }
    payload["credited_record_count"] = int(len(primary_rows.index))
    payload["credited_credit_total"] = float(primary_rows["Credit Request Total"].sum()) if not primary_rows.empty else 0.0
    suggestions = meta.get("suggestions") if isinstance(meta.get("suggestions"), list) else []
    return payload, suggestions


def _minimal_overall_summary_response(
    *,
    resolved_window: str,
    start_naive: pd.Timestamp | None,
    end_naive: pd.Timestamp | None,
    open_count: int,
    open_total: float,
    credited_metrics: dict[str, Any],
    rtn_suggestions: list[dict[str, str]],
) -> tuple[str, None, dict[str, Any]]:
    time_metrics = _empty_time_metrics()
    suggestions: list[dict[str, str]] = []
    ops_snapshot_suggestion = _ops_snapshot_suggestion(start_naive, end_naive)
    if ops_snapshot_suggestion is not None:
        suggestions.append(ops_snapshot_suggestion)
    for item in rtn_suggestions:
        if isinstance(item, dict) and item.get("prefix") and item not in suggestions:
            suggestions.append(item)
    plot_suggestion = _credit_amount_plot_suggestion(start_naive, end_naive)
    if plot_suggestion not in suggestions:
        suggestions.append(plot_suggestion)

    message_lines = [
        "📊 **Credit Overview**",
        f"- Window: **{resolved_window}**",
        "",
        "💸 **Liability Snapshot**",
        f"- Open exposure: **{format_money(open_total)}** across **{open_count}** record(s)",
        "",
        "✅ **What Was Credited In Period**",
        f"- What was credited in period: **{format_money(credited_metrics['credited_credit_total'])}** across **{credited_metrics['credited_record_count']}** unique record(s)",
        f"- Primary attribution: **system-led {credited_metrics['primary_system_record_count']} / {format_money(credited_metrics['primary_system_credit_total'])}**, **manual-led {credited_metrics['primary_manual_record_count']} / {format_money(credited_metrics['primary_manual_credit_total'])}**",
        f"- Avg days from entry to RTN assignment: **{credited_metrics['avg_days_to_rtn_assignment']:.1f}**",
        f"- Reopened after terminal: **{credited_metrics['reopened_after_terminal_count']}** record(s)",
    ]

    meta = {
        "show_table": False,
        "suggestions": suggestions,
        "overall_summary": {
            "window": resolved_window,
            "open_record_count": open_count,
            "open_credit_total": open_total,
            "avg_days_open": time_metrics["avg_days_open"],
            "avg_days_since_last_status": time_metrics["avg_days_since_last_status"],
            "billing_queue_delay_count": time_metrics["billing_queue_delay_count"],
            "billing_queue_delay_total": time_metrics["billing_queue_delay_total"],
            "stale_investigation_count": time_metrics["stale_investigation_count"],
            "stale_investigation_total": time_metrics["stale_investigation_total"],
            "credited_in_period": credited_metrics,
            "top_customers": [],
            "top_items": [],
            "top_root_causes": [],
        },
    }

    return "\n".join(message_lines), None, meta


def intent_overall_summary(query: str, df: pd.DataFrame):
    q_low = query.lower()
    keywords_any = ["summary", "overview", "picture", "status", "how are credits", "credit overview"]
    if not any(k in q_low for k in keywords_any):
        return None

    if df.empty:
        return "I don't see any credit records to summarize right now."

    dv = df.copy()
    if isinstance(getattr(df, "attrs", None), dict):
        dv.attrs = dict(df.attrs)
    if "Date" in dv.columns:
        dv["Date"] = coerce_date(dv["Date"])
    else:
        dv["Date"] = pd.NaT

    if "Credit Request Total" in dv.columns:
        dv["Credit Request Total_num"] = pd.to_numeric(dv["Credit Request Total"], errors="coerce").fillna(0.0)
    else:
        dv["Credit Request Total_num"] = 0.0

    start, end, window_label = _parse_window(query)
    scope = dv.copy()
    start_naive = _naive_ts(start)
    end_naive = _naive_ts(end)

    if start_naive is not None and end_naive is not None and "Date" in scope.columns:
        mask = scope["Date"].between(start_naive, end_naive)
        scope = scope[mask].copy()
    elif start_naive is not None and "Date" in scope.columns:
        scope = scope[scope["Date"] >= start_naive].copy()

    if scope.empty:
        label = window_label or "that window"
        return f"I couldn't find any credit records for {label}."

    resolved_window = _window_range_label(start_naive, end_naive) or window_label or "current dataset"

    if "RTN_CR_No" in scope.columns:
        open_mask = ~_has_rtn(scope["RTN_CR_No"])
    else:
        open_mask = pd.Series(True, index=scope.index)

    open_df = scope[open_mask].copy()
    open_count = int(len(open_df.index))
    open_total = float(open_df["Credit Request Total_num"].sum())

    try:
        time_metrics = _time_reasoning_metrics(open_df.dropna(subset=["Date"]).copy()) if "Date" in open_df.columns else _time_reasoning_metrics(pd.DataFrame())
        credited_metrics, rtn_suggestions = _credited_in_period_metrics(dv, start=start_naive, end=end_naive)

        top_customers = _top_amount_groups(scope, "Customer Number")
        top_items = _top_amount_groups(scope, "Item Number")
        top_root_causes = _cached_root_cause_summary(dv) or _root_cause_summary(scope)

        message_lines = [
            "📊 **Credit Overview**",
            f"- Window: **{resolved_window}**",
            "",
            "💸 **Liability Snapshot**",
            f"- Open exposure: **{format_money(open_total)}** across **{open_count}** record(s)",
            "",
            "⏱️ **Time reasoning**",
            f"- Avg days open: **{time_metrics['avg_days_open']:.1f}**",
            f"- Avg days since last update: **{time_metrics['avg_days_since_last_status']:.1f}**",
            f"- Billing queue delay: **{time_metrics['billing_queue_delay_count']}** record(s) / **{format_money(time_metrics['billing_queue_delay_total'])}**",
            f"- Stale investigation: **{time_metrics['stale_investigation_count']}** record(s) / **{format_money(time_metrics['stale_investigation_total'])}**",
            "",
            "✅ **What Was Credited In Period**",
            f"- What was credited in period: **{format_money(credited_metrics['credited_credit_total'])}** across **{credited_metrics['credited_record_count']}** unique record(s)",
            f"- Primary attribution: **system-led {credited_metrics['primary_system_record_count']} / {format_money(credited_metrics['primary_system_credit_total'])}**, **manual-led {credited_metrics['primary_manual_record_count']} / {format_money(credited_metrics['primary_manual_credit_total'])}**",
            f"- Avg days from entry to RTN assignment: **{credited_metrics['avg_days_to_rtn_assignment']:.1f}**",
            f"- Reopened after terminal: **{credited_metrics['reopened_after_terminal_count']}** record(s)",
        ]

        if top_customers:
            message_lines.extend(["", "🏢 **Mix / Drivers**", "Top customers in scope:"])
            for item in top_customers:
                message_lines.append(f"- **{item['label']}** — {format_money(item['credit_total'])}")

        if top_items:
            message_lines.append("")
            message_lines.append("Top items in scope:")
            for item in top_items:
                message_lines.append(f"- **{item['label']}** — {format_money(item['credit_total'])}")

        if top_root_causes:
            message_lines.append("")
            message_lines.append("Main root causes in scope:")
            for item in top_root_causes:
                message_lines.append(
                    f"- **{item['root_cause']}** — **{item['record_count']}** record(s) / {format_money(item['credit_total'])}"
                )

        suggestions = []
        ops_snapshot_suggestion = _ops_snapshot_suggestion(start_naive, end_naive)
        if ops_snapshot_suggestion is not None:
            suggestions.append(ops_snapshot_suggestion)
        for item in rtn_suggestions:
            if isinstance(item, dict) and item.get("prefix") and item not in suggestions:
                suggestions.append(item)
        plot_suggestion = _credit_amount_plot_suggestion(start_naive, end_naive)
        if plot_suggestion not in suggestions:
            suggestions.append(plot_suggestion)

        meta = {
            "show_table": False,
            "suggestions": suggestions,
            "overall_summary": {
                "window": resolved_window,
                "open_record_count": open_count,
                "open_credit_total": open_total,
                "avg_days_open": time_metrics["avg_days_open"],
                "avg_days_since_last_status": time_metrics["avg_days_since_last_status"],
                "billing_queue_delay_count": time_metrics["billing_queue_delay_count"],
                "billing_queue_delay_total": time_metrics["billing_queue_delay_total"],
                "stale_investigation_count": time_metrics["stale_investigation_count"],
                "stale_investigation_total": time_metrics["stale_investigation_total"],
                "credited_in_period": credited_metrics,
                "top_customers": top_customers,
                "top_items": top_items,
                "top_root_causes": top_root_causes,
            },
        }

        return "\n".join(message_lines), None, meta
    except Exception:
        try:
            credited_metrics, rtn_suggestions = _credited_in_period_metrics(dv, start=start_naive, end=end_naive)
        except Exception:
            credited_metrics, rtn_suggestions = _empty_credited_metrics(), []
        return _minimal_overall_summary_response(
            resolved_window=resolved_window,
            start_naive=start_naive,
            end_naive=end_naive,
            open_count=open_count,
            open_total=open_total,
            credited_metrics=credited_metrics,
            rtn_suggestions=rtn_suggestions,
        )
