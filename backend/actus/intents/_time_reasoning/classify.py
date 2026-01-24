"""Classification logic for time reasoning enrichment."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional

from .status_map import (
    CLOSED,
    INTAKE,
    INVESTIGATION,
    NOT_PROCESSED,
    ON_HOLD,
    RESOLVED_PENDING_CLOSE,
    SUBMITTED_TO_BILLING,
)
from .thresholds import aging_not_submitted_days, stale_days_by_phase


INTENT_I01 = "I01_NUDGE_INTAKE_OWNER"
INTENT_I02 = "I02_REQUEST_MISSING_INFO"
INTENT_I03 = "I03_ESCALATE_STALE_INVESTIGATION"
INTENT_I04 = "I04_CHECK_BILLING_QUEUE"
INTENT_I05 = "I05_CONFIRM_RESOLUTION_CLOSURE"
INTENT_I06 = "I06_IGNORE_SYSTEM_NOISE"
INTENT_I07 = "I07_DEPRIORITIZE_ACTIVE_PROGRESS"
INTENT_I08 = "I08_FLAG_AGING_NOT_SUBMITTED"

FOLLOWUP_INTENTS = {INTENT_I01, INTENT_I02, INTENT_I03, INTENT_I04, INTENT_I08}


@dataclass(frozen=True)
class Classification:
    """Container for classification outputs."""

    follow_up_intent: Optional[str]
    delay_category: str
    delay_reason: str
    delay_score: float
    checkpoint_at: Optional[str]


def _delay_score(
    *,
    days_open: float,
    days_since_touch: float,
    is_big: bool,
    category: str,
) -> float:
    base = min(100.0, (days_open / 90.0) * 50.0 + (days_since_touch / 30.0) * 50.0)
    if is_big:
        base = min(100.0, base * 1.2)

    if category in {"TERMINAL", "SYSTEM_NOISE"}:
        return 0.0

    if category == "NORMAL":
        return min(15.0, base)

    return min(100.0, base)


def _checkpoint_at(
    *,
    now: datetime,
    phase: str,
    intent: Optional[str],
) -> Optional[str]:
    if intent not in FOLLOWUP_INTENTS:
        return None

    delta_days = 2 if phase == ON_HOLD else 1
    return (now + timedelta(days=delta_days)).isoformat()


def classify_row(
    phase: str,
    days_open: float,
    days_since_touch: float,
    credit_total: float,
    is_system_noise: bool,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, object]:
    """Classify a ticket row into delay and follow-up intent.

    Args:
        phase: Macro phase.
        days_open: Days open.
        days_since_touch: Days since last status update.
        credit_total: Total credit exposure.
        is_system_noise: Whether status is system-generated noise.
        now: Reference time for follow-up checkpoint.

    Returns:
        Dict with Follow_Up_Intent, Delay_Category, Delay_Reason, Delay_Score, Checkpoint_At.
    """
    now = now or datetime.utcnow()
    is_big = credit_total >= 2500.0
    stale_threshold = stale_days_by_phase(phase, is_big=is_big)
    aging_threshold = aging_not_submitted_days(is_big=is_big)

    if is_system_noise:
        return Classification(
            follow_up_intent=INTENT_I06,
            delay_category="SYSTEM_NOISE",
            delay_reason="System update only.",
            delay_score=0.0,
            checkpoint_at=None,
        ).__dict__

    if phase in {CLOSED, NOT_PROCESSED}:
        return Classification(
            follow_up_intent=None,
            delay_category="TERMINAL",
            delay_reason="Terminal status.",
            delay_score=0.0,
            checkpoint_at=None,
        ).__dict__

    if phase == RESOLVED_PENDING_CLOSE:
        return Classification(
            follow_up_intent=INTENT_I05,
            delay_category="RESOLVED_PENDING_CLOSE",
            delay_reason="Resolved; pending auto-close window.",
            delay_score=10.0,
            checkpoint_at=None,
        ).__dict__

    if phase == SUBMITTED_TO_BILLING and days_since_touch > stale_threshold:
        score = _delay_score(
            days_open=days_open,
            days_since_touch=days_since_touch,
            is_big=is_big,
            category="BILLING_QUEUE_DELAY",
        )
        reason = f"Submitted to billing but no movement for {days_since_touch:.0f} days."
        intent = INTENT_I04
        return Classification(
            follow_up_intent=intent,
            delay_category="BILLING_QUEUE_DELAY",
            delay_reason=reason,
            delay_score=score,
            checkpoint_at=_checkpoint_at(now=now, phase=phase, intent=intent),
        ).__dict__

    if phase in {INTAKE, INVESTIGATION, ON_HOLD} and days_open > aging_threshold:
        score = _delay_score(
            days_open=days_open,
            days_since_touch=days_since_touch,
            is_big=is_big,
            category="AGING_NOT_SUBMITTED",
        )
        score = max(60.0, score)
        reason = f"Open {days_open:.0f} days and not submitted to billing."
        intent = INTENT_I08
        return Classification(
            follow_up_intent=intent,
            delay_category="AGING_NOT_SUBMITTED",
            delay_reason=reason,
            delay_score=score,
            checkpoint_at=_checkpoint_at(now=now, phase=phase, intent=intent),
        ).__dict__

    if phase in {INTAKE, INVESTIGATION, ON_HOLD} and days_since_touch > stale_threshold:
        if phase == INTAKE:
            intent = INTENT_I01
        elif phase == INVESTIGATION:
            intent = INTENT_I03
        else:
            intent = INTENT_I02

        score = _delay_score(
            days_open=days_open,
            days_since_touch=days_since_touch,
            is_big=is_big,
            category="STALE_IN_PHASE",
        )
        reason = f"No updates in {days_since_touch:.0f} days while in {phase}."
        return Classification(
            follow_up_intent=intent,
            delay_category="STALE_IN_PHASE",
            delay_reason=reason,
            delay_score=score,
            checkpoint_at=_checkpoint_at(now=now, phase=phase, intent=intent),
        ).__dict__

    if days_open > 45 and days_since_touch <= 3:
        score = _delay_score(
            days_open=days_open,
            days_since_touch=days_since_touch,
            is_big=is_big,
            category="ACTIVE_PROGRESS",
        )
        score = max(30.0, min(55.0, score))
        return Classification(
            follow_up_intent=INTENT_I07,
            delay_category="ACTIVE_PROGRESS",
            delay_reason="Old ticket but recently updated; actively moving.",
            delay_score=score,
            checkpoint_at=None,
        ).__dict__

    score = _delay_score(
        days_open=days_open,
        days_since_touch=days_since_touch,
        is_big=is_big,
        category="NORMAL",
    )
    return Classification(
        follow_up_intent=None,
        delay_category="NORMAL",
        delay_reason="Within expected time.",
        delay_score=score,
        checkpoint_at=None,
    ).__dict__
