import pytest

from ducat_lakehouse.config import load_pipeline_config, load_rules
from ducat_lakehouse.rules import (
    UNCATEGORIZED,
    Rule,
    ValidationError,
    apply_rules,
    categorize,
    make_rule,
    sort_rules,
)

RULES = [
    Rule(30, "merchant", "regex", "cafe|pizza", "Dining"),
    Rule(10, "description", "contains", "payroll", "Income"),
    Rule(20, "merchant", "equals", "Blue Door Cafe", "Coffee"),
    Rule(40, "description", "starts_with", "transfer ", "Transfer"),
]


def test_first_match_by_priority_wins_regardless_of_list_order():
    assert apply_rules("Blue Door Cafe", "BLUE DOOR CAFE CARD PURCHASE", RULES) == "Coffee"
    assert apply_rules("Blue Door Cafe", "BLUE DOOR CAFE CARD PURCHASE", list(reversed(RULES))) == "Coffee"


def test_lower_priority_rule_applies_when_higher_does_not_match():
    assert apply_rules("Ember Pizza", "EMBER PIZZA CARD PURCHASE", RULES) == "Dining"


def test_priority_beats_specificity_across_fields():
    assert apply_rules("Payroll Cafe", "NORTHWIND PAYROLL DIRECT DEP", RULES) == "Income"


@pytest.mark.parametrize(
    "merchant, description, expected",
    [
        ("x", "Northwind PAYROLL", "Income"),
        ("blue door cafe", "", "Coffee"),
        ("Harbor Bank", "TRANSFER TO SAVINGS", "Transfer"),
        ("Harbor Bank", "INSTANT TRANSFER TO SAVINGS", None),
        ("Blue Door Cafe Annex", "", "Dining"),
    ],
)
def test_operators_are_case_insensitive_and_exact(merchant, description, expected):
    assert apply_rules(merchant, description, RULES) == expected


def test_no_match_returns_none_and_categorize_falls_back():
    assert apply_rules("Corner Pantry", "CORNER PANTRY CARD PURCHASE", RULES) is None
    assert categorize(None, "Corner Pantry", "CORNER PANTRY", RULES) == UNCATEGORIZED


def test_missing_fields_do_not_match_or_crash():
    assert apply_rules(None, None, RULES) is None


def test_user_category_is_never_overwritten():
    assert categorize("Travel", "Blue Door Cafe", "BLUE DOOR CAFE", RULES) == "Travel"
    assert categorize("Groceries", "Harbor Bank", "TRANSFER TO SAVINGS", RULES) == "Groceries"


def test_null_user_category_is_categorized_by_rules():
    assert categorize(None, "Blue Door Cafe", "BLUE DOOR CAFE", RULES) == "Coffee"


def test_duplicate_priorities_are_rejected():
    with pytest.raises(ValidationError, match="duplicate rule priority 10"):
        sort_rules([Rule(10, "merchant", "equals", "a", "A"), Rule(10, "merchant", "equals", "b", "B")])


@pytest.mark.parametrize(
    "entry, message",
    [
        ({"priority": 1, "field": "memo", "operator": "equals", "value": "a", "category": "A"}, "field must be"),
        ({"priority": 1, "field": "merchant", "operator": "like", "value": "a", "category": "A"}, "operator must be"),
        ({"priority": 1, "field": "merchant", "operator": "regex", "value": "(", "category": "A"}, "invalid regex"),
        ({"priority": "1", "field": "merchant", "operator": "equals", "value": "a", "category": "A"}, "priority"),
        ({"priority": 1, "field": "merchant", "operator": "equals", "value": "", "category": "A"}, "value"),
        ({"priority": 1, "field": "merchant", "operator": "equals", "value": "a"}, "missing keys"),
    ],
)
def test_malformed_rules_are_rejected(entry, message):
    with pytest.raises(ValidationError, match=message):
        make_rule(entry)


def test_shipped_rules_load_and_categorize_generated_merchants():
    rules = load_rules()
    assert [r.priority for r in rules] == sorted(r.priority for r in rules)
    assert categorize(None, "FitHub Gym", "FITHUB GYM RECURRING", rules) == "Health & Fitness"
    assert categorize(None, "Streamly", "STREAMLY RECURRING", rules) == "Subscriptions"
    assert categorize(None, "Harvest Grocer", "HARVEST GROCER CARD PURCHASE", rules) == "Groceries"
    assert categorize(None, "Corner Pantry", "CORNER PANTRY CARD PURCHASE", rules) == UNCATEGORIZED


def test_shipped_pipeline_config_loads_with_overrides():
    cfg = load_pipeline_config(catalog="workspace", schema="dev_someone_ducat")
    assert cfg.transfer_window_days == 4
    assert cfg.anomaly_z_threshold == 3
    assert cfg.table("silver_transactions") == "workspace.dev_someone_ducat.silver_transactions"


def test_pipeline_config_rejects_unsafe_identifiers():
    with pytest.raises(ValidationError, match="not a plain identifier"):
        load_pipeline_config(schema="ducat; DROP TABLE x")
