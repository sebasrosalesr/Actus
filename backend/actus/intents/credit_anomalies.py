import re
import pandas as pd

from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "anomalies",
    "anomaly",
    "anomaly detection",
    "anomaly dection",
    "anomaly scan",
    "unusual credits",
    "suspicious credits",
    "outliers",
]

SHORTCUT_ALIASES = (
    "anomalies",
    "anomaly",
    "anomaly detection",
    "anomaly dection",
    "anomaly scan",
    "outliers",
)


def intent_credit_anomalies(query: str, df: pd.DataFrame):
    """
    Handle queries like:
      - "Any anomalies in credits?"
      - "Actus, show unusual credits"
      - "Are there suspicious credit amounts lately?"

    Logic:
      - Look at last 90 days
      - Use Credit Request Total (numeric)
      - Compute z-score on recent credits
      - Flag rows if EITHER:
          (A) abs(amount) >= 500 AND |z| >= 3   (statistical anomaly)
          (B) abs(amount) >= 2500               (management hard cap)
    """

    q_low = query.lower()
    normalized = re.sub(r"[^a-z0-9\s]", " ", q_low)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    # Trigger words
    if not (
        "anomal" in q_low
        or "unusual" in q_low
        or "suspicious" in q_low
        or "outlier" in q_low
        or "weird" in q_low
    ):
        return None

    shortcut_request = len(normalized.split()) <= 4 and any(
        alias in normalized for alias in SHORTCUT_ALIASES
    )

    if "credit" not in q_low and "ticket" not in q_low and not shortcut_request:
        # Let other intents try if it's not clearly about credits
        return None

    # Required columns
    if "Date" not in df.columns or "Credit Request Total" not in df.columns:
        return (
            "I can't run anomaly detection because I need both `Date` and "
            "`Credit Request Total` columns."
        )

    dv = df.copy()
    dv["Date"] = coerce_date(dv["Date"])
    dv = dv.dropna(subset=["Date"])

    if dv.empty:
        return "I don't have any dated records to run anomaly detection."

    # Last 90 days window
    latest = dv["Date"].max().normalize()
    cutoff = latest - pd.Timedelta(days=90)
    recent = dv[dv["Date"].between(cutoff, latest)].copy()

    if recent.empty:
        return "There are no credit records in the last 90 days to analyze."

    # Numeric credits
    recent["Credit Request Total"] = pd.to_numeric(
        recent["Credit Request Total"], errors="coerce"
    ).fillna(0.0)

    if recent["Credit Request Total"].std() == 0:
        return "All recent credits are roughly the same size – no clear anomalies."

    # z-score
    mu = recent["Credit Request Total"].mean()
    sigma = recent["Credit Request Total"].std()
    recent["z_score"] = (recent["Credit Request Total"] - mu) / sigma

    # ---- Anomaly rules ----
    base_amount_threshold = 500.0   # for z-score-based anomalies
    z_threshold = 3.0
    hard_cap = 2500.0               # 🔹 management hard cap

    abs_amt = recent["Credit Request Total"].abs()

    cond_z = (abs_amt >= base_amount_threshold) & (recent["z_score"].abs() >= z_threshold)
    cond_cap = abs_amt >= hard_cap

    # Flag reason for each anomaly
    recent["anomaly_reason"] = ""
    recent.loc[cond_z & ~cond_cap, "anomaly_reason"] = "statistical"
    recent.loc[cond_cap & ~cond_z, "anomaly_reason"] = "hard_cap"
    recent.loc[cond_cap & cond_z, "anomaly_reason"] = "both"

    anomalies = recent[cond_z | cond_cap].copy()

    if anomalies.empty:
        return (
            "I don't see any large or statistically unusual credits in the last 90 days "
            f"(amount ≥ {format_money(base_amount_threshold)} with |z| ≥ {z_threshold}, "
            f"or any credits ≥ {format_money(hard_cap)})."
        )

    total_anom = len(anomalies)
    total_anom_amt = anomalies["Credit Request Total"].sum()

    # Grouped views
    # 1) By customer
    if "Customer Number" in anomalies.columns:
        by_cust = (
            anomalies.groupby("Customer Number", dropna=False)["Credit Request Total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
    else:
        by_cust = pd.Series(dtype=float)

    # 2) By item
    if "Item Number" in anomalies.columns:
        by_item = (
            anomalies.groupby("Item Number", dropna=False)["Credit Request Total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
    else:
        by_item = pd.Series(dtype=float)

    # 3) By sales rep
    if "Sales Rep" in anomalies.columns:
        by_rep = (
            anomalies.groupby("Sales Rep", dropna=False)["Credit Request Total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
    else:
        by_rep = pd.Series(dtype=float)

    # Top raw anomaly rows (sorted by |z|)
    anomalies["abs_z"] = anomalies["z_score"].abs()
    top_rows = anomalies.sort_values("abs_z", ascending=False).head(15)

    lines: list[str] = []

    lines.append("🚨 **Credit Anomaly Scan – Last 90 Days**")
    lines.append(f"- Window analyzed: **{cutoff.date()} → {latest.date()}**")
    lines.append(
        f"- Anomalous credits found: **{total_anom}** "
        f"totalling **{format_money(total_anom_amt)}**"
    )
    lines.append(
        f"- Rules:\n"
        f"  - Statistically unusual: amount ≥ {format_money(base_amount_threshold)} "
        f"with |z-score| ≥ {z_threshold:.1f}\n"
        f"  - Management hard cap: any credit ≥ {format_money(hard_cap)}"
    )
    lines.append("")

    # Group summaries
    if not by_cust.empty:
        lines.append("👥 **Top customers with anomalous credits:**")
        for cust, val in by_cust.items():
            label = cust if pd.notna(cust) else "UNKNOWN"
            lines.append(f"- {label}: {format_money(val)} in anomalies")
        lines.append("")

    if not by_item.empty:
        lines.append("📦 **Top items with anomalous credits:**")
        for item, val in by_item.items():
            label = item if pd.notna(item) else "UNKNOWN"
            lines.append(f"- Item {label}: {format_money(val)} in anomalies")
        lines.append("")

    if not by_rep.empty:
        lines.append("🧑‍💼 **Top sales reps with anomalous credits:**")
        for rep, val in by_rep.items():
            label = rep if pd.notna(rep) else "UNKNOWN"
            lines.append(f"- {label}: {format_money(val)} in anomalies")
        lines.append("")

    reason_map = {
        "statistical": "Statistical",
        "hard_cap": "Hard cap",
        "both": "Both",
    }

    preview = top_rows.copy()
    preview["Anomaly Flag"] = preview["anomaly_reason"].map(reason_map).fillna("Unknown")
    preview = preview.rename(columns={"z_score": "Z Score"})

    preview_cols = [
        "Date",
        "Ticket Number",
        "Customer Number",
        "Item Number",
        "Sales Rep",
        "Credit Request Total",
        "Z Score",
        "Anomaly Flag",
    ]
    preview_cols = [col for col in preview_cols if col in preview.columns]
    preview = preview[preview_cols]

    csv_rows = anomalies.copy()
    csv_rows["Anomaly Flag"] = csv_rows["anomaly_reason"].map(reason_map).fillna("Unknown")
    csv_rows = csv_rows.rename(columns={"z_score": "Z Score"})
    csv_cols = [
        "Date",
        "Ticket Number",
        "Customer Number",
        "Item Number",
        "Sales Rep",
        "Credit Request Total",
        "Z Score",
        "Anomaly Flag",
    ]
    csv_cols = [col for col in csv_cols if col in csv_rows.columns]
    csv_rows = csv_rows[csv_cols]

    lines.append("🔍 **Most extreme anomalous credits (top 15 by |z-score|):**")
    lines.append("Here is a preview of the most extreme anomalies with flags for hard cap vs statistical.")
    return (
        "\n".join(lines),
        preview,
        {
            "csv_rows": csv_rows,
            "csv_row_count": int(len(csv_rows)),
            "columns": preview_cols,
            "csv_filename": "credit_anomalies.csv",
        },
    )
