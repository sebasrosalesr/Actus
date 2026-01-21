import re
import pandas as pd

from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "record lookup",
    "lookup record",
    "is ticket",
    "do we have invoice",
]


def _norm(series: pd.Series) -> pd.Series:
    """Normalize IDs for comparison."""
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "", regex=False)
    )


def _extract_ids(query: str) -> list[str]:
    """
    Pull possible ticket / invoice IDs out of the query text.
    Examples it will catch:
      - R-040699
      - r-045013
      - INV14068709
      - 14068709  (plain number, often invoice)
    """
    ids: set[str] = set()

    # Ticket style: R-123456 or r-1234
    for m in re.findall(r"\bR-\d{3,}\b", query, flags=re.IGNORECASE):
        ids.add(m.upper())

    # Invoice style: INV + digits
    for m in re.findall(r"\bINV-?\d{4,}\b", query, flags=re.IGNORECASE):
        ids.add(m.upper().replace("-", ""))

    # Plain long numbers (6+ digits) – likely invoices
    for m in re.findall(r"\b\d{6,}\b", query):
        ids.add(m)

    return list(ids)


def intent_record_lookup(query: str, df: pd.DataFrame) -> tuple[str, pd.DataFrame] | str | None:
    """
    Handle queries like:
      - "Is ticket R-040699 logged in the system?"
      - "Do we have invoice 14068709 in the credit file?"
      - "Is invoice INV14068709 on record?"

    It checks Ticket Number and Invoice Number and reports
    whether we have any matching rows, plus a short summary.
    """
    q_low = query.lower()

    # Only trigger when the user is asking about existence / being logged
    keywords = ["logged", "in the system", "on record", "on file", "do we have", "exist"]
    if not any(k in q_low for k in keywords):
        return None

    # Must look like a ticket / invoice question
    if "ticket" not in q_low and "invoice" not in q_low and "credit" not in q_low:
        # Let other intents try
        return None

    # Need at least one of these columns to do anything
    has_ticket_col = "Ticket Number" in df.columns
    has_invoice_col = "Invoice Number" in df.columns
    if not (has_ticket_col or has_invoice_col):
        return (
            "I can't check whether a record is logged because I don't see "
            "`Ticket Number` or `Invoice Number` columns in the dataset."
        )

    ids = _extract_ids(query)
    if not ids:
        # No ID found – let other intents maybe handle it
        return None

    # Normalized columns
    ticket_norm = _norm(df["Ticket Number"]) if has_ticket_col else None
    invoice_norm = _norm(df["Invoice Number"]) if has_invoice_col else None

    combined_matches: list[pd.DataFrame] = []
    summary_parts: list[str] = []

    for rid in ids:
        masks = []

        if has_ticket_col:
            masks.append(ticket_norm == rid)

        if has_invoice_col:
            # Accept exact invoice ID or with INV prefix stripped
            rid_clean = rid.replace("INV", "").replace("INV-", "")
            invoice_mask = (invoice_norm == rid) | (invoice_norm == rid_clean)
            masks.append(invoice_mask)

        if not masks:
            continue

        mask_all = masks[0]
        for m in masks[1:]:
            mask_all = mask_all | m

        found_df = df[mask_all].copy()
        count = len(found_df)

        if count == 0:
            summary_parts.append(f"{rid}: not found")
            continue

        # Optional money summary
        total_amt = None
        if "Credit Request Total" in found_df.columns:
            total_amt = pd.to_numeric(
                found_df["Credit Request Total"], errors="coerce"
            ).sum()

        summary = f"{rid}: {count} record(s)"
        if total_amt is not None:
            summary += f", total {format_money(total_amt)}"
        summary_parts.append(summary)

        combined_matches.append(found_df)

    if not combined_matches:
        message = "Record lookup: no matching tickets or invoices found."
        return message

    merged = pd.concat(combined_matches, ignore_index=True)
    if "Record ID" in merged.columns:
        merged = merged.drop_duplicates(subset=["Record ID"])
    elif "id" in merged.columns:
        merged = merged.drop_duplicates(subset=["id"])
    else:
        merged = merged.drop_duplicates()
    merged = merged.copy()
    merged["Sort Time"] = pd.to_datetime(merged.get("Date"), errors="coerce")
    completeness_cols = [
        col
        for col in [
            "Customer Number",
            "Invoice Number",
            "Ticket Number",
            "Credit Request Total",
            "Reason for Credit",
            "Date",
            "Status",
        ]
        if col in merged.columns
    ]
    if completeness_cols:
        merged["Completeness"] = merged[completeness_cols].notna().sum(axis=1)
    else:
        merged["Completeness"] = 0
    merged = merged.sort_values(
        ["Completeness", "Sort Time"], ascending=[False, False], na_position="last"
    )
    latest = merged.head(5)

    if "Status" in latest.columns:
        def _latest_status(value: str) -> str:
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

        latest = latest.assign(**{"Latest Status": latest["Status"].map(_latest_status)})

    preferred_cols = [
        "Date",
        "Customer Number",
        "Invoice Number",
        "Ticket Number",
        "Credit Request Total",
        "Latest Status",
        "Reason for Credit",
    ]
    fallback_values = {}
    for col in preferred_cols:
        if col in merged.columns:
            series = merged[col].dropna()
            if not series.empty:
                fallback_values[col] = series.iloc[0]
    for col in preferred_cols:
        if col not in latest.columns:
            latest[col] = None
    latest = latest[preferred_cols]
    for col, value in fallback_values.items():
        if col in latest.columns:
            latest[col] = latest[col].where(latest[col].notna(), value)

    subset = latest.copy()
    message = "Record lookup snapshot — " + "; ".join(summary_parts)
    if len(subset) > 1:
        message += f" (showing latest {len(subset)} records)"
    return message, subset, {"columns": preferred_cols}
