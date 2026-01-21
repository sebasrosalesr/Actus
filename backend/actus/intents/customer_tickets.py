import re
import pandas as pd

# If you put these helpers in utils, import them like this:
from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money
from actus.utils.matching import normalize

INTENT_ALIASES = [
    "customer tickets",
    "tickets for customer",
    "customer history",
]


def intent_customer_tickets(query: str, df: pd.DataFrame):
    """
    Handle queries like:
      - "SkyBar, show all tickets for customer YAM"
      - "show all tickets for customer YAM in last 30 days"

    Uses prefix + contains on Customer Number (YAM -> YAM01, YAM33, ...)
    """
    q_low = query.lower()
    if "customer" not in q_low or "ticket" not in q_low:
        return None  # not my intent

    m_cust = re.search(
        r"customer(?:\s+number)?\s+([A-Za-z0-9_-]+)",
        query,
        flags=re.IGNORECASE,
    )
    if not m_cust:
        return (
            "I heard 'customer', but I couldn’t detect which one. "
            "Try: `show all tickets for customer YAM`."
        )

    cust_token = normalize(m_cust.group(1))  # e.g. "YAM"

    if "Customer Number" not in df.columns:
        return "I can't find a `Customer Number` column in the dataset."

    dv = df.copy()
    if "Date" in dv.columns:
        dv["Date"] = coerce_date(dv["Date"])

    cust_series = dv["Customer Number"].astype(str).str.upper().str.strip()

    # prefix & contains (so YAM -> YAM01, YAM33, and any number containing YAM)
    prefix_mask = cust_series.str.startswith(cust_token, na=False)
    contains_mask = cust_series.str.contains(cust_token, na=False)
    cust_mask = prefix_mask | contains_mask

    # time windows
    if "last 15" in q_low:
        days = 15
    elif "last 30" in q_low:
        days = 30
    else:
        days = None

    if days and "Date" in dv.columns:
        today = pd.Timestamp.today().normalize()
        cutoff = today - pd.Timedelta(days=days)
        subset = dv[cust_mask & dv["Date"].between(cutoff, today)].copy()
    else:
        subset = dv[cust_mask].copy()

    if subset.empty:
        if days:
            return (
                f"I don't see any tickets for customers matching **'{cust_token}'** "
                f"in the last {days} days."
            )
        return (
            f"I don't see any tickets in the database for customers matching "
            f"**'{cust_token}'**."
        )

    subset = subset.sort_values("Date", ascending=False)

    total_tickets = len(subset)
    total_credits = pd.to_numeric(
        subset["Credit Request Total"], errors="coerce"
    ).sum()
    total_credits_str = format_money(total_credits)

    distinct_customers = (
        subset["Customer Number"]
        .astype(str)
        .str.upper()
        .dropna()
        .unique()
        .tolist()
    )
    cust_display = ", ".join(distinct_customers[:10])
    if len(distinct_customers) > 10:
        cust_display += f", ... (+{len(distinct_customers) - 10} more)"

    header = (
        f"Here’s what I found for customers matching **'{cust_token}'**"
        f"{f' in the last {days} days' if days else ''}:"
    )

    message = "\n".join(
        [
            header,
            f"- Matching customer numbers: **{cust_display}**",
            f"- Tickets: **{total_tickets}**",
            f"- Sum of `Credit Request Total`: **{total_credits_str}**",
            "",
            "Here is a preview of the results.",
        ]
    )

    preview = subset.head(20).copy()

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": f"customer_{cust_token}_tickets.csv",
            "csv_rows": subset,
            "csv_row_count": len(subset),
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
