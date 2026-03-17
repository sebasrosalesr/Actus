from __future__ import annotations

import json
from pathlib import Path
import re

from .models import RootCauseMatch, RootCauseRule


def normalize_text(text: str | None) -> str:
    return " ".join(str(text or "").lower().strip().split())


def _default_rules_path() -> Path:
    return Path(__file__).resolve().parents[3] / "config" / "root_cause_rules.json"


def load_root_cause_rules(path: str | Path | None = None) -> list[RootCauseRule]:
    rules_path = Path(path) if path else _default_rules_path()
    with open(rules_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    rules: list[RootCauseRule] = []
    for row in data:
        rules.append(
            RootCauseRule(
                id=str(row.get("id") or "").strip(),
                label=str(row.get("label") or row.get("id") or "").strip(),
                priority=int(row.get("priority") or 0),
                threshold=max(1, int(row.get("threshold") or 1)),
                keywords=[str(k).lower().strip() for k in (row.get("keywords") or []) if str(k).strip()],
                negative_keywords=[
                    str(k).lower().strip() for k in (row.get("negative_keywords") or []) if str(k).strip()
                ],
            )
        )
    return rules


def score_root_causes(text: str, rules: list[RootCauseRule]) -> list[RootCauseMatch]:
    value = normalize_text(text)
    matches: list[RootCauseMatch] = []

    for rule in rules:
        if not rule.id:
            continue
        if any(neg in value for neg in rule.negative_keywords):
            continue
        triggers = [kw for kw in rule.keywords if kw and kw in value]
        count = len(triggers)
        if count < rule.threshold:
            continue

        matches.append(
            RootCauseMatch(
                id=rule.id,
                label=rule.label,
                priority=rule.priority,
                count=count,
                score=float(count) / float(rule.threshold),
                triggers=sorted(set(triggers)),
            )
        )

    return matches


def _infer_fallback_root_cause(
    texts: list[str],
    rules: list[RootCauseRule],
) -> tuple[str, list[str]] | None:
    value = normalize_text(" ".join(texts))
    if not value:
        return None

    known_ids = {rule.id for rule in rules if rule.id}
    if not known_ids:
        return None

    # Keep fallback deterministic and conservative:
    # only infer when explicit lexical signals are present.
    if "freight_error" in known_ids and re.search(r"\bfreight\b|\bshipping\b|\bhandling\b|\bdelivery\b", value):
        return ("freight_error", ["fallback:freight/shipping"])

    if "ppd_mismatch" in known_ids and re.search(r"\bnon[-\s]?ppd\b|\bppd\b", value):
        return ("ppd_mismatch", ["fallback:ppd/non-ppd"])

    if "sub_price_mismatch" in known_ids and re.search(
        r"\bsub\w*\b|\bsubstitute\b|\bsubstitution\b|\bnot price matched\b|\bprice match\b",
        value,
    ):
        return ("sub_price_mismatch", ["fallback:sub/price-match"])

    if "price_loaded_after_invoice" in known_ids and (
        "crediting after item was invoiced" in value
        or (
            re.search(r"\bprice\b|\bpricing\b|\bpriced\b", value)
            and (
                re.search(r"\bupdated\b|\bcorrected\b|\breverted\b|\bcontract\b", value)
                or re.search(r"\binvoice\b|\binvoiced\b", value)
            )
        )
    ):
        return ("price_loaded_after_invoice", ["fallback:price+invoice/update"])

    if "price_discrepancy" in known_ids and re.search(
        r"(\bprice\b|\bpricing\b|\bpriced\b).*(\bwrong\b|\bincorrect\b|\bnot correct\b|\bdifferential\b|\bincrease\b)"
        r"|(\bwrong\b|\bincorrect\b|\bnot correct\b|\bdifferential\b|\bincrease\b).*(\bprice\b|\bpricing\b|\bpriced\b)",
        value,
    ):
        return ("price_discrepancy", ["fallback:price-discrepancy"])

    return None


def detect_root_causes(texts: list[str], rules: list[RootCauseRule]) -> dict[str, object]:
    aggregated: dict[str, RootCauseMatch] = {}
    rule_priority = {rule.id: int(rule.priority) for rule in rules if rule.id}
    counts: dict[str, int] = {}

    for text in texts:
        if not text:
            continue
        for match in score_root_causes(text, rules):
            current = aggregated.get(match.id)
            if current is None:
                aggregated[match.id] = RootCauseMatch(
                    id=match.id,
                    label=match.label,
                    priority=match.priority,
                    count=match.count,
                    score=match.score,
                    triggers=list(match.triggers),
                )
                counts[match.id] = counts.get(match.id, 0) + match.count
                continue

            merged_triggers = sorted(set(current.triggers + match.triggers))
            aggregated[match.id] = RootCauseMatch(
                id=current.id,
                label=current.label,
                priority=max(current.priority, match.priority),
                count=current.count + match.count,
                score=current.score + match.score,
                triggers=merged_triggers,
            )
            counts[match.id] = counts.get(match.id, 0) + match.count
    sorted_root_causes = sorted(
        aggregated.keys(),
        key=lambda cid: (
            -int(rule_priority.get(cid, 0)),
            -int(counts.get(cid, 0)),
            cid,
        ),
    )

    if not sorted_root_causes:
        fallback = _infer_fallback_root_cause(texts, rules)
        if fallback is not None:
            fallback_id, fallback_triggers = fallback
            label_by_id = {rule.id: rule.label for rule in rules if rule.id}
            fallback_label = label_by_id.get(fallback_id, fallback_id)
            return {
                "root_cause_ids": [fallback_id],
                "root_cause_labels": [fallback_label],
                "root_cause_primary_id": fallback_id,
                "root_cause_primary_label": fallback_label,
                "root_cause_triggers": fallback_triggers,
                "root_cause_score": 0.5,
            }
        return {
            "root_cause_ids": [],
            "root_cause_labels": [],
            "root_cause_primary_id": "unidentified",
            "root_cause_primary_label": "Unidentified",
            "root_cause_triggers": [],
            "root_cause_score": 0.0,
        }

    primary_id = sorted_root_causes[0]
    primary = aggregated[primary_id]
    return {
        "root_cause_ids": sorted_root_causes,
        "root_cause_labels": [aggregated[cid].label for cid in sorted_root_causes],
        "root_cause_primary_id": primary.id,
        "root_cause_primary_label": primary.label,
        "root_cause_triggers": primary.triggers,
        "root_cause_score": primary.score,
    }
