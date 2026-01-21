import pandas as pd

from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money
from actus.utils.matching import normalize

INTENT_ALIASES = [
    "credits with rtn",
    "credit number",
    "credits have credit number",
]


def intent_rtn_summary(query: str, df: pd.DataFrame):
    """
    Handle queries like:
      - "How many credits have a credit number?"
      - "Show me records with RTN_CR_No"
      - "SkyBar, which credits have RTNs?"
    """
    q_low = query.lower()

    # Intent trigger rules
    if (
        ("credit number" not in q_low)
        and ("rtn_cr_no" not in q_low)
        and not ("rtn" in q_low and "credit" in q_low)
        and "credit number" not in q_low
    ):
        return None  # Not for this intent

    colname = "RTN_CR_No"
    if colname not in df.columns:
        return "I can’t find the `RTN_CR_No` column in the dataset."

    dv = df.copy()

    wants_missing = any(
        phrase in q_low
        for phrase in [
            "don't have",
            "do not have",
            "without",
            "no credit number",
            "missing credit number",
            "without credit number",
            "no rtn",
        ]
    )

    # Non-empty RTN filter
    rtn_col = dv[colname].astype(str).str.strip()
    valid_mask = rtn_col.ne("") & ~rtn_col.str.upper().isin(["NAN", "NONE", "NULL"])
    rtn_df = dv[valid_mask].copy()
    missing_df = dv[~valid_mask].copy()

    if wants_missing and missing_df.empty:
        return "I don’t see any credits missing a `RTN_CR_No` yet."
    if not wants_missing and rtn_df.empty:
        return "I don’t see any credits with a populated `RTN_CR_No` yet."

    target_df = missing_df if wants_missing else rtn_df
    total_with_rtn = len(target_df)
    total_credits = pd.to_numeric(
        target_df.get("Credit Request Total"), errors="coerce"
    ).sum()
    total_credits_str = format_money(total_credits)

    # Clean up dates for sorting
    if "Date" in target_df.columns:
        target_df["Date"] = coerce_date(target_df["Date"])
        target_df = target_df.sort_values("Date", ascending=False)

    message = "\n".join(
        [
            (
                f"I currently see **{total_with_rtn}** credit request(s) "
                f"{'missing' if wants_missing else 'with a non-empty'} **RTN_CR_No**."
            ),
            f"- Sum of `Credit Request Total`: **{total_credits_str}**",
            "",
            "Here is a preview of the results.",
        ]
    )

    preview = target_df.head(20).copy()

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "credits_missing_rtn.csv" if wants_missing else "credits_with_rtn.csv",
            "csv_rows": target_df,
            "csv_row_count": len(target_df),
            "columns": [
                "Date",
                "Ticket Number",
                "Customer Number",
                "Invoice Number",
                "Item Number",
                "QTY",
                "Unit Price",
                "Corrected Unit Price",
                "Credit Type",
                "Credit Request Total",
                "Issue Type",
                "Reason for Credit",
                "Requested By",
                "EDI Service Provider",
                "Status",
                "RTN_CR_No",
                "Type",
                "Sales Rep",
            ],
        },
    )
