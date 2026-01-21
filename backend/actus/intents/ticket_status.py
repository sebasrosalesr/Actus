import re
import pandas as pd

from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "ticket status",
    "status on ticket",
    "status of ticket",
    "ticket update",
]


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


def intent_ticket_status(query, df):
    """
    Handles questions like:
      - Actus, status on ticket R-040699
      - Show me ticket R-045013
      - What's happening with R-050155?
    
    Now upgraded:
      ✔ Returns ALL rows for the ticket
      ✔ Still provides a clean summary at the top
    """

    r_ids = {m.upper() for m in re.findall(r"\bR-\d{3,}\b", query, flags=re.IGNORECASE)}
    r_digits = {re.sub(r"\D", "", tid) for tid in r_ids}
    numeric_ids = {m for m in re.findall(r"\b\d{5,}\b", query) if m not in r_digits}
    tickets = list(r_ids.union(numeric_ids))
    if not tickets:
        return None
    ticket_series = df["Ticket Number"].astype(str).str.upper().str.strip()
    ticket_digits = ticket_series.str.replace(r"\D", "", regex=True)
    df = df.copy()
    df["Ticket Number"] = ticket_series

    if len(tickets) > 1:
        found_rows = []
        missing = []
        full_matches = []

        for ticket in tickets:
            ticket_upper = ticket.upper()
            ticket_num = re.sub(r"\D", "", ticket_upper)
            mask = (ticket_series == ticket_upper) | (ticket_digits == ticket_num)
            ticket_rows = df[mask].copy()
            if ticket_rows.empty:
                missing.append(ticket)
                found_rows.append(
                    {
                        "Ticket Number": ticket,
                        "Customer Number": "N/A",
                        "Invoice Number": "N/A",
                        "Credit Request Total": None,
                        "Latest Status": "Not found",
                        "Reason for Credit": "",
                        "Date": None,
                    }
                )
                continue

            if "Date" in ticket_rows.columns:
                ticket_rows = ticket_rows.sort_values("Date", ascending=True)
                latest = ticket_rows.tail(1)
            else:
                latest = ticket_rows.head(1)

            latest = latest.copy()
            found_rows.append(latest.iloc[0].to_dict())
            full_matches.append(ticket_rows)

        result_df = pd.DataFrame(found_rows)
        full_df = pd.concat(full_matches, ignore_index=True) if full_matches else result_df
        message = f"Here is a preview of the {len(tickets)} ticket(s) provided."
        if missing:
            message += f"\nNot found: {', '.join(sorted(missing))}"
        return message, result_df, {
            "show_table": True,
            "csv_filename": "ticket_status_snapshot.csv",
            "csv_rows": full_df,
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
        }

    ticket = tickets[0]
    ticket_upper = ticket.upper()
    ticket_num = re.sub(r"\D", "", ticket_upper)
    mask = (ticket_series == ticket_upper) | (ticket_digits == ticket_num)
    all_rows = df[mask].copy()

    if all_rows.empty:
        return f"I couldn't find any records for ticket **{ticket}**."

    if "Date" in all_rows.columns:
        all_rows = all_rows.sort_values("Date", ascending=True)

    first = all_rows.iloc[0]

    date = first.get("Date")
    date_str = date.strftime("%Y-%m-%d") if isinstance(date, pd.Timestamp) else "Unknown date"

    total_sum = None
    if "Credit Request Total" in all_rows.columns:
        total_sum = pd.to_numeric(all_rows["Credit Request Total"], errors="coerce").sum()

    subset = all_rows.head(20)
    message = (
        f"Here is a snapshot of ticket **{ticket}** "
        f"({len(all_rows)} entries, first seen {date_str})."
    )

    return message, subset
