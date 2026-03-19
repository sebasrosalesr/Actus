import re
import pandas as pd

from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "bulk search",
    "bulk lookup",
    "bulk search invoices",
]


def _norm(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "", regex=False)
        .str.replace("-", "", regex=False)
    )


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    deduped = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _extract_tokens(query: str) -> list[str]:
    query = re.sub(r"^\s*actus\b[:,]?\s*", "", query, flags=re.IGNORECASE)
    stopwords = {
        "actus",
        "bulk",
        "search",
        "lookup",
        "find",
        "list",
        "lists",
        "of",
        "for",
        "please",
        "show",
        "me",
        "these",
        "the",
        "and",
        "or",
        "numbers",
        "number",
        "invoice",
        "invoices",
        "item",
        "items",
        "customer",
        "customers",
        "cust",
        "rtn",
        "rtncr",
        "credit",
        "cr",
    }

    cleaned = re.sub(r"[\n;]+", ",", query)
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    tokens: list[str] = []

    for part in parts:
        part = re.sub(
            r"^(?:invoices?|items?|customers?|cust(?:omer)?s?|rtn(?:_cr_no)?|credit numbers?)\b[:\-]?\s*",
            "",
            part,
            flags=re.IGNORECASE,
        )
        for token in re.split(r"\s+", part):
            token = token.strip().strip("()[]{}.,:;!?\"'")
            if not token:
                continue
            if token.lower() in stopwords:
                continue
            if re.search(r"\d", token):
                tokens.append(token)
            elif token.isalpha() and 2 <= len(token) <= 5:
                tokens.append(token)

    return _dedupe(tokens)


def _find_item_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "Item Number",
        "Item",
        "Item ID",
        "Item Code",
        "Item #",
        "ItemNum",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _find_invoice_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "Invoice Number",
        "Invoice",
        "Invoice #",
        "Inv Number",
        "Inv #",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _find_customer_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "Customer Number",
        "Customer",
        "Customer ID",
        "Cust Number",
        "Cust #",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _find_rtn_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "RTN_CR_No",
        "RTN",
        "Credit Number",
        "Credit No",
        "CR Number",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def intent_bulk_search(query: str, df: pd.DataFrame):
    """
    Handle queries like:
      - "Bulk search these invoices: 14068709, 14068710"
      - "Find these item numbers 1201, 1202, 1304"
      - "Bulk lookup RTNCR numbers: 98765, 98766"
      - "Search these customer numbers: YAM, LCL"
    """
    q_low = query.lower()

    bulk_keywords = [
        "bulk",
        "batch",
        "list",
        "multiple",
        "lookup",
        "search",
        "find",
    ]
    type_keywords = ["invoice", "item", "rtn", "rtncr", "credit number", "customer", "cust"]

    if not any(k in q_low for k in bulk_keywords):
        return None
    if not any(k in q_low for k in type_keywords):
        return None

    tokens = _extract_tokens(query)
    if len(tokens) < 2:
        return (
            "I can run a bulk search, but I need a list of values (comma-separated or line-separated)."
        )
    has_list_delim = bool(re.search(r"[,\n;]", query)) or ":" in query
    has_digit_token = any(re.search(r"\d", token) for token in tokens)
    wants_customer = any(k in q_low for k in ["customer", "customers", "cust"])
    alpha_tokens = [t for t in tokens if t.isalpha() and 2 <= len(t) <= 5]
    has_alpha_code_list = wants_customer and len(alpha_tokens) >= 2
    if not (has_list_delim or has_digit_token or has_alpha_code_list):
        return (
            "I can run a bulk search, but I need a list of values (comma-separated or line-separated)."
        )

    wants_invoice = any(k in q_low for k in ["invoice", "invoices", "inv"])
    wants_item = any(k in q_low for k in ["item", "items", "sku", "product"])
    wants_rtn = any(k in q_low for k in ["rtn", "rtncr", "credit number", "credit no", "cr number"])

    if not any([wants_invoice, wants_item, wants_customer, wants_rtn]):
        wants_invoice = wants_item = wants_customer = wants_rtn = True

    invoice_col = _find_invoice_column(df) if wants_invoice else None
    item_col = _find_item_column(df) if wants_item else None
    customer_col = _find_customer_column(df) if wants_customer else None
    rtn_col = _find_rtn_column(df) if wants_rtn else None

    available = {
        "Invoice Number": invoice_col,
        "Item Number": item_col,
        "Customer Number": customer_col,
        "RTN_CR_No": rtn_col,
    }
    available = {label: col for label, col in available.items() if col}

    if not available:
        return (
            "I can't run bulk search because I don't see invoice, item, customer, "
            "or RTN columns in the dataset."
        )

    matches: list[pd.DataFrame] = []
    found_by_type: dict[str, set[str]] = {label: set() for label in available}
    missing_by_type: dict[str, set[str]] = {label: set() for label in available}

    for label, col in available.items():
        col_norm = _norm(df[col])
        for token in tokens:
            token_norm = token.strip().upper().replace(" ", "").replace("-", "")
            token_variants = {token_norm}
            if label == "Invoice Number" and token_norm.startswith("INV"):
                token_variants.add(token_norm.replace("INV", "", 1))

            if label == "Customer Number":
                match_mask = col_norm.str.startswith(token_norm)
            else:
                match_mask = col_norm.isin(token_variants)

            if match_mask.any():
                found_by_type[label].add(token)
                subset = df[match_mask].copy()
                subset["Match Type"] = label
                subset["Match Value"] = token
                matches.append(subset)
            else:
                missing_by_type[label].add(token)

    if not matches:
        missing_display = []
        for label, missing in missing_by_type.items():
            if missing:
                missing_display.append(f"{label}: {', '.join(sorted(missing))}")
        missing_text = "; ".join(missing_display) if missing_display else "no matches"
        return f"Bulk search results: {missing_text}."

    combined = pd.concat(matches, ignore_index=True)

    if "Date" in combined.columns:
        combined = combined.sort_values("Date", ascending=False)

    if "Credit Request Total" in combined.columns:
        combined["Credit Request Total"] = pd.to_numeric(
            combined["Credit Request Total"], errors="coerce"
        )
        total_amt = combined["Credit Request Total"].sum()
    else:
        total_amt = None

    summary_lines = ["Bulk search results:"]
    summary_lines.append(f"- Total matches: {len(combined)} record(s)")
    for label, found in found_by_type.items():
        if found:
            summary_lines.append(
                f"- {label}: {len(found)} matched: {', '.join(sorted(found))}"
            )
    if total_amt is not None:
        summary_lines.append(f"- Sum of `Credit Request Total`: {format_money(total_amt)}")
    summary_lines.append("")
    summary_lines.append("Here is a preview of the results.")

    preferred_cols = [
        "Match Type",
        "Match Value",
        "Customer Number",
        "Invoice Number",
        "Item Number",
        "RTN_CR_No",
        "Ticket Number",
        "Credit Request Total",
        "Reason for Credit",
        "Date",
        "Status",
    ]
    for col in preferred_cols:
        if col not in combined.columns:
            combined[col] = None
    preview = combined[preferred_cols].head(25).copy()

    return (
        "\n".join(summary_lines),
        preview,
        {
            "show_table": True,
            "csv_filename": "bulk_search_results.csv",
            "csv_rows": combined,
            "csv_row_count": len(combined),
            "columns": preferred_cols,
        },
    )
