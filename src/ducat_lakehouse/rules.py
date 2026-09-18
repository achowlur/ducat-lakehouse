"""Pure-Python rule matching, input validators, transfer pairing and the anomaly baseline.

Nothing here imports Spark: silver.py and gold.py express the same semantics as
Spark expressions, and the tests pin the semantics here.
"""

from __future__ import annotations

import math
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping, Sequence

FIELDS = ("merchant", "description")
OPERATORS = ("contains", "equals", "starts_with", "regex")
UNCATEGORIZED = "Uncategorized"


class ValidationError(ValueError):
    pass


@dataclass(frozen=True)
class Rule:
    priority: int
    field: str
    operator: str
    value: str
    category: str


def make_rule(entry: Mapping) -> Rule:
    keys = {"priority", "field", "operator", "value", "category"}
    missing = sorted(keys - set(entry))
    unknown = sorted(set(entry) - keys)
    if missing or unknown:
        raise ValidationError(f"rule {dict(entry)!r}: missing keys {missing}, unknown keys {unknown}")
    if isinstance(entry["priority"], bool) or not isinstance(entry["priority"], int):
        raise ValidationError(f"rule {dict(entry)!r}: priority must be an integer")
    if entry["field"] not in FIELDS:
        raise ValidationError(f"rule {dict(entry)!r}: field must be one of {FIELDS}")
    if entry["operator"] not in OPERATORS:
        raise ValidationError(f"rule {dict(entry)!r}: operator must be one of {OPERATORS}")
    for key in ("value", "category"):
        if not isinstance(entry[key], str) or not entry[key]:
            raise ValidationError(f"rule {dict(entry)!r}: {key} must be a non-empty string")
    if entry["operator"] == "regex":
        try:
            re.compile(entry["value"])
        except re.error as exc:
            raise ValidationError(f"rule {dict(entry)!r}: invalid regex ({exc})") from exc
    return Rule(entry["priority"], entry["field"], entry["operator"], entry["value"], entry["category"])


def sort_rules(rules: Iterable[Rule]) -> list[Rule]:
    ordered = sorted(rules, key=lambda rule: rule.priority)
    seen = set()
    for rule in ordered:
        if rule.priority in seen:
            raise ValidationError(f"duplicate rule priority {rule.priority}; priorities must be unique")
        seen.add(rule.priority)
    return ordered


def rule_matches(rule: Rule, merchant: str | None, description: str | None) -> bool:
    text = (merchant if rule.field == "merchant" else description) or ""
    if rule.operator == "contains":
        return rule.value.lower() in text.lower()
    if rule.operator == "equals":
        return text.lower() == rule.value.lower()
    if rule.operator == "starts_with":
        return text.lower().startswith(rule.value.lower())
    return re.search(rule.value, text, re.IGNORECASE) is not None


def apply_rules(merchant: str | None, description: str | None, rules: Iterable[Rule]) -> str | None:
    for rule in sorted(rules, key=lambda r: r.priority):
        if rule_matches(rule, merchant, description):
            return rule.category
    return None


def categorize(user_category: str | None, merchant: str | None, description: str | None, rules: Iterable[Rule]) -> str:
    if user_category is not None:
        return user_category
    return apply_rules(merchant, description, rules) or UNCATEGORIZED


def check_columns(actual: Iterable[str], expected: Iterable[str], table: str) -> None:
    actual, expected = list(actual), list(expected)
    missing = [c for c in expected if c not in actual]
    unexpected = [c for c in actual if c not in expected]
    if missing or unexpected:
        raise ValidationError(f"{table}: missing columns {missing}, unexpected columns {unexpected}")


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value == "":
        return True
    return type(value).__name__ in ("NAType", "NaTType")


def _parse_decimal(value) -> Decimal:
    parsed = Decimal(str(value).strip())
    if not parsed.is_finite():
        raise ValueError("not finite")
    return parsed


def _parse_date(value) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text not in ("true", "false"):
        raise ValueError("not a boolean")
    return text == "true"


def _parse_int(value) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer")
    if isinstance(value, float) and not value.is_integer():
        raise ValueError("not an integer")
    return int(str(value).strip()) if isinstance(value, str) else int(value)


_PARSERS = {
    "string": str,
    "int": _parse_int,
    "decimal": _parse_decimal,
    "date": _parse_date,
    "bool": _parse_bool,
}


def check_types(frame, expected_types: Mapping[str, str], table: str, nullable: Sequence[str] = ()) -> None:
    for column, type_name in expected_types.items():
        if type_name not in _PARSERS:
            raise ValidationError(f"{table}.{column}: unknown expected type {type_name!r}")
        if column not in frame:
            raise ValidationError(f"{table}.{column}: column not present")
        parse = _PARSERS[type_name]
        for position, value in enumerate(frame[column]):
            if _is_missing(value):
                if column in nullable:
                    continue
                raise ValidationError(f"{table}.{column}: null at row {position}, column is not nullable")
            try:
                parse(value)
            except (ValueError, TypeError, InvalidOperation) as exc:
                raise ValidationError(
                    f"{table}.{column}: {value!r} at row {position} is not a valid {type_name}"
                ) from exc


def check_signed_amounts(amounts: Iterable, table: str) -> None:
    values = list(amounts)
    if not values:
        raise ValidationError(f"{table}.amount: no rows to validate")
    parsed = []
    for position, value in enumerate(values):
        if _is_missing(value):
            raise ValidationError(f"{table}.amount: null at row {position}")
        try:
            amount = _parse_decimal(value)
        except (ValueError, TypeError, InvalidOperation) as exc:
            raise ValidationError(f"{table}.amount: {value!r} at row {position} is not numeric") from exc
        if amount == 0:
            raise ValidationError(f"{table}.amount: zero at row {position}; every transaction moves money")
        parsed.append(amount)
    if all(amount > 0 for amount in parsed):
        raise ValidationError(
            f"{table}.amount: all {len(parsed)} amounts are positive; the feed looks unsigned "
            "(expected positive inflows and negative outflows)"
        )


def _cents(value) -> int:
    return int((_parse_decimal(value) * 100).to_integral_value())


def pair_transfers(transactions: Iterable[Mapping], window_days: int = 4) -> dict[str, str]:
    """Pair opposite amounts across two accounts of one user within window_days.

    Rows with a user_category are never paired. Each outflow picks its closest
    inflow (ties by transaction_id) and each inflow its closest outflow; a pair
    stands only when the choice is mutual. Returns transaction_id -> partner id
    in both directions.
    """
    eligible = [t for t in transactions if t.get("user_category") is None]
    inflows = defaultdict(list)
    for row in eligible:
        if _cents(row["amount"]) > 0:
            inflows[(row["user_id"], _cents(row["amount"]))].append(row)

    best_for_out: dict[str, tuple[int, str]] = {}
    best_for_in: dict[str, tuple[int, str]] = {}
    for out in eligible:
        cents = _cents(out["amount"])
        if cents >= 0:
            continue
        for inflow in inflows[(out["user_id"], -cents)]:
            if inflow["account_id"] == out["account_id"]:
                continue
            gap = abs((_parse_date(inflow["posted_date"]) - _parse_date(out["posted_date"])).days)
            if gap > window_days:
                continue
            out_id, in_id = out["transaction_id"], inflow["transaction_id"]
            if out_id not in best_for_out or (gap, in_id) < best_for_out[out_id]:
                best_for_out[out_id] = (gap, in_id)
            if in_id not in best_for_in or (gap, out_id) < best_for_in[in_id]:
                best_for_in[in_id] = (gap, out_id)

    partners = {}
    for out_id, (_, in_id) in best_for_out.items():
        if best_for_in[in_id][1] == out_id:
            partners[out_id] = in_id
            partners[in_id] = out_id
    return partners


def outflow_z_score(
    magnitude: float, recent: Sequence[float], history: Sequence[float], min_history: int = 5
) -> float | None:
    """Reference for gold.score_anomalies: the recent window when it holds min_history
    outflows, otherwise all prior history; None when neither baseline qualifies or it
    has no spread. history includes recent. Uses the sample standard deviation.
    """
    baseline = recent if len(recent) >= min_history else history
    if len(baseline) < min_history:
        return None
    spread = statistics.stdev(baseline)
    if spread == 0:
        return None
    return (magnitude - statistics.fmean(baseline)) / spread
