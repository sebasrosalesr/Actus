"""Thresholds for staleness and aging logic."""
from __future__ import annotations

from typing import Dict

from .status_map import INTAKE, INVESTIGATION, ON_HOLD, SUBMITTED_TO_BILLING


_BASE_STALE_DAYS: Dict[str, int] = {
    INTAKE: 5,
    INVESTIGATION: 7,
    ON_HOLD: 10,
    SUBMITTED_TO_BILLING: 7,
}


def stale_days_by_phase(phase: str, *, is_big: bool) -> int:
    """Return staleness threshold days for a given phase.

    Args:
        phase: Macro phase string.
        is_big: Whether exposure is big.

    Returns:
        Threshold in days.
    """
    base = _BASE_STALE_DAYS.get(phase, 7)
    if is_big:
        return max(2, base - 2)
    return base


def aging_not_submitted_days(*, is_big: bool) -> int:
    """Return aging threshold for tickets not submitted to billing.

    Args:
        is_big: Whether exposure is big.

    Returns:
        Threshold in days.
    """
    return 14 if is_big else 30
