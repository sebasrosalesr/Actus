from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class RuleDecision:
    rule_id: str
    next_action: str
    action_confidence: str
    action_reason_codes: List[str]
    action_tag: Optional[str] = None


_RULES_CACHE: Dict[str, Any] = {"mtime": None, "rules": None}


def _default_rules_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "next_action_rules.json"


def _load_rules(path: Path) -> List[Dict[str, Any]]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return [r for r in data if isinstance(r, dict)]


def _get_rules(path: Optional[Path] = None) -> List[Dict[str, Any]]:
    rules_path = path or _default_rules_path()
    try:
        mtime = rules_path.stat().st_mtime
    except FileNotFoundError:
        return []

    cached_mtime = _RULES_CACHE.get("mtime")
    if cached_mtime == mtime and _RULES_CACHE.get("rules") is not None:
        return _RULES_CACHE["rules"]

    rules = _load_rules(rules_path)
    _RULES_CACHE["mtime"] = mtime
    _RULES_CACHE["rules"] = rules
    return rules


def _compare(field_value: Any, op: str, expected: Any) -> bool:
    if op == "truthy":
        return bool(field_value)
    if op == "falsy":
        return not bool(field_value)
    if op == "eq":
        return field_value == expected
    if op == "ne":
        return field_value != expected
    if field_value is None:
        return False
    try:
        if op == "gt":
            return field_value > expected
        if op == "gte":
            return field_value >= expected
        if op == "lt":
            return field_value < expected
        if op == "lte":
            return field_value <= expected
    except Exception:
        return False
    if op == "in":
        try:
            return field_value in expected
        except Exception:
            return False
    if op == "contains":
        try:
            return expected in field_value
        except Exception:
            return False
    return False


def _eval_cond(cond: Dict[str, Any], context: Dict[str, Any]) -> bool:
    if "all" in cond:
        return all(_eval_cond(c, context) for c in cond["all"])
    if "any" in cond:
        return any(_eval_cond(c, context) for c in cond["any"])
    if "not" in cond:
        return not _eval_cond(cond["not"], context)
    if "signal" in cond:
        return bool(context["signals"].get(cond["signal"]))
    if "flag" in cond:
        return bool(context["flags"].get(cond["flag"]))
    if "field" in cond:
        field = cond.get("field")
        op = cond.get("op", "truthy")
        expected = cond.get("value")
        return _compare(context["fields"].get(field), op, expected)
    return False


def _eval_cond_trace(cond: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    if "all" in cond:
        children = [_eval_cond_trace(c, context) for c in cond["all"]]
        return {
            "type": "all",
            "result": all(c["result"] for c in children),
            "children": children,
        }
    if "any" in cond:
        children = [_eval_cond_trace(c, context) for c in cond["any"]]
        return {
            "type": "any",
            "result": any(c["result"] for c in children),
            "children": children,
        }
    if "not" in cond:
        child = _eval_cond_trace(cond["not"], context)
        return {"type": "not", "result": not child["result"], "child": child}
    if "signal" in cond:
        key = cond["signal"]
        return {
            "type": "signal",
            "signal": key,
            "result": bool(context["signals"].get(key)),
        }
    if "flag" in cond:
        key = cond["flag"]
        return {
            "type": "flag",
            "flag": key,
            "result": bool(context["flags"].get(key)),
        }
    if "field" in cond:
        field = cond.get("field")
        op = cond.get("op", "truthy")
        expected = cond.get("value")
        value = context["fields"].get(field)
        return {
            "type": "field",
            "field": field,
            "op": op,
            "value": value,
            "expected": expected,
            "result": _compare(value, op, expected),
        }
    return {"type": "unknown", "result": False}


def _sorted_rules(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(rule: Dict[str, Any]) -> tuple[int, str]:
        priority = int(rule.get("priority", 0) or 0)
        rule_id = str(rule.get("id", ""))
        return (-priority, rule_id)

    return sorted(rules, key=_key)


def evaluate_next_action(
    context: Dict[str, Any],
    *,
    rules_path: Optional[Path] = None,
) -> Optional[RuleDecision]:
    rules = _get_rules(rules_path)
    if not rules:
        return None

    for rule in _sorted_rules(rules):
        when = rule.get("when")
        if not isinstance(when, dict):
            continue
        if not _eval_cond(when, context):
            continue

        action = rule.get("action") or {}
        next_action = action.get("next_action")
        if not next_action:
            continue

        confidence = action.get("confidence", "medium")
        reason_codes = action.get("reason_codes") or []
        if not isinstance(reason_codes, list):
            reason_codes = [str(reason_codes)]
        action_tag = action.get("tag")
        return RuleDecision(
            rule_id=str(rule.get("id") or ""),
            next_action=str(next_action),
            action_confidence=str(confidence),
            action_reason_codes=[str(c) for c in reason_codes],
            action_tag=str(action_tag) if action_tag else None,
        )

    return None


def evaluate_next_action_with_trace(
    context: Dict[str, Any],
    *,
    rules_path: Optional[Path] = None,
) -> tuple[Optional[RuleDecision], List[Dict[str, Any]]]:
    rules = _get_rules(rules_path)
    trace: List[Dict[str, Any]] = []
    if not rules:
        return None, trace

    decision: Optional[RuleDecision] = None
    for rule in _sorted_rules(rules):
        when = rule.get("when")
        if not isinstance(when, dict):
            trace.append(
                {
                    "id": str(rule.get("id") or ""),
                    "priority": int(rule.get("priority", 0) or 0),
                    "matched": False,
                    "error": "missing_when",
                }
            )
            continue

        when_trace = _eval_cond_trace(when, context)
        matched = bool(when_trace["result"])
        trace.append(
            {
                "id": str(rule.get("id") or ""),
                "priority": int(rule.get("priority", 0) or 0),
                "matched": matched,
                "when": when_trace,
                "action": rule.get("action"),
            }
        )

        if matched and decision is None:
            action = rule.get("action") or {}
            next_action = action.get("next_action")
            if next_action:
                confidence = action.get("confidence", "medium")
                reason_codes = action.get("reason_codes") or []
                if not isinstance(reason_codes, list):
                    reason_codes = [str(reason_codes)]
                action_tag = action.get("tag")
                decision = RuleDecision(
                    rule_id=str(rule.get("id") or ""),
                    next_action=str(next_action),
                    action_confidence=str(confidence),
                    action_reason_codes=[str(c) for c in reason_codes],
                    action_tag=str(action_tag) if action_tag else None,
                )

    return decision, trace
