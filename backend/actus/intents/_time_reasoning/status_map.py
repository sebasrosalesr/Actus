"""Status mapping for time reasoning enrichment."""
from __future__ import annotations

import re
from typing import Tuple


INTAKE = "INTAKE"
INVESTIGATION = "INVESTIGATION"
ON_HOLD = "ON_HOLD"
SUBMITTED_TO_BILLING = "SUBMITTED_TO_BILLING"
RESOLVED_PENDING_CLOSE = "RESOLVED_PENDING_CLOSE"
CLOSED = "CLOSED"
NOT_PROCESSED = "NOT_PROCESSED"
SYSTEM_NOISE = "SYSTEM_NOISE"
UNKNOWN = "UNKNOWN"


def macro_phase_from_status(status: str | None) -> Tuple[str, bool]:
    """Map a status message to a macro phase and system-noise flag.

    Args:
        status: Raw status message string or None.

    Returns:
        Tuple of (macro_phase, is_system_noise).
    """
    if not status:
        return UNKNOWN, False

    s = str(status).strip()
    s = re.sub(r"^\[?\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]?\s*", "", s).strip()
    s_upper = re.sub(r"\s+", " ", s.upper())

    if s_upper.startswith("[SYSTEM] UPDATED"):
        return SYSTEM_NOISE, True

    if "NOT PROCESSED" in s_upper or "WILL NOT BE PROCESSED" in s_upper:
        return NOT_PROCESSED, False

    if "CLOSED:" in s_upper or s_upper == "CLOSED: COMPLETED.":
        return CLOSED, False

    if ("CREDIT NUMBER" in s_upper or "CREDIT NUMBERS" in s_upper) and (
        "WILL CLOSE" in s_upper and "14 DAYS" in s_upper
    ):
        return RESOLVED_PENDING_CLOSE, False

    if "SUBMITTED TO BILLING" in s_upper:
        return SUBMITTED_TO_BILLING, False

    if "ON HOLD:" in s_upper:
        return ON_HOLD, False

    if "ON MACRO" in s_upper or "INVESTIGATION" in s_upper:
        return INVESTIGATION, False

    if s_upper.startswith("PENDING:"):
        return INTAKE, False

    return UNKNOWN, False
