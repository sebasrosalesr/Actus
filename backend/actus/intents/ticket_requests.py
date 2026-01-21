import re
import pandas as pd

INTENT_ALIASES = [
    "ticket requests",
    "requests for ticket",
    "ticket request list",
]

def intent_ticket_requests(query: str, df: pd.DataFrame):
    """
    Example trigger phrases:
      - "show me all the requests for ticket R-040699"
    """
    m = re.search(r"(R-\d+)", query, flags=re.IGNORECASE)
    if not m:
        return None
    
    ticket = m.group(1).upper()
    df_match = df[df["Ticket Number"].astype(str).str.upper() == ticket].copy()

    if df_match.empty:
        return (f"No rows found for **{ticket}**.", None)

    total_rows = len(df_match)
    total_credit = pd.to_numeric(df_match["Credit Request Total"], errors="coerce").sum()

    text = (
        f"### 📄 All entries for ticket **{ticket}**\n"
        f"- Rows found: **{total_rows}**\n"
        f"- Total Credit Request Total: **${total_credit:,.2f}**"
    )

    return (text, df_match)
