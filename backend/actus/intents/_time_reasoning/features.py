"""Feature extraction helpers for time reasoning."""
from __future__ import annotations

from typing import List

import pandas as pd


def get_col(df: pd.DataFrame, names: List[str]) -> str | None:
    """Return first matching column name from a list.

    Args:
        df: Input DataFrame.
        names: Candidate column names in priority order.

    Returns:
        Column name if found, else None.
    """
    for name in names:
        if name in df.columns:
            return name
    return None


def safe_numeric(series: pd.Series) -> pd.Series:
    """Coerce a series to floats, filling invalids with 0.

    Args:
        series: Input series.

    Returns:
        Float series with NaN coerced to 0.
    """
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def compute_days_open(df: pd.DataFrame) -> pd.Series:
    """Compute days open from available columns.

    Args:
        df: Input DataFrame.

    Returns:
        Series of days open (float).
    """
    col = get_col(df, ["Days_Open", "DAYS OPEN"])
    if col is None:
        return pd.Series([0.0] * len(df), index=df.index, dtype=float)
    return safe_numeric(df[col])


def compute_days_since_touch(df: pd.DataFrame) -> pd.Series:
    """Compute days since last status update from available columns.

    Args:
        df: Input DataFrame.

    Returns:
        Series of days since last status update (float).
    """
    col = get_col(df, ["Days_Since_Last_Status", "DAYS SINCE LAST STATUS"])
    if col is None:
        return pd.Series([0.0] * len(df), index=df.index, dtype=float)
    return safe_numeric(df[col])
