import pandas as pd
import pytest

from ducat_lakehouse.rules import ValidationError, check_columns, check_signed_amounts, check_types
from ducat_lakehouse.schemas import TABLES

TXN = TABLES["transactions"]


def good_frame(**overrides):
    row = {
        "user_id": "u000001",
        "account_id": "u000001-chk",
        "transaction_id": "u000001-000001",
        "posted_date": "2026-03-01",
        "amount": "-12.50",
        "merchant": "Blue Door Cafe",
        "description": "BLUE DOOR CAFE CARD PURCHASE",
        "user_category": None,
        "is_planted_anomaly": "false",
    }
    row.update(overrides)
    return pd.DataFrame([row, {**row, "transaction_id": "u000001-000002", "amount": "3000.00"}])


def test_check_columns_accepts_exact_set_in_any_order():
    check_columns(list(reversed(TXN["columns"])), TXN["columns"], "transactions")


def test_check_columns_reports_missing():
    columns = [c for c in TXN["columns"] if c != "amount"]
    with pytest.raises(ValidationError, match=r"transactions: missing columns \['amount'\], unexpected columns \[\]"):
        check_columns(columns, TXN["columns"], "transactions")


def test_check_columns_reports_unexpected():
    with pytest.raises(ValidationError, match=r"unexpected columns \['memo'\]"):
        check_columns([*TXN["columns"], "memo"], TXN["columns"], "transactions")


def test_check_types_accepts_a_valid_pandas_head():
    check_types(good_frame(), TXN["columns"], "transactions", TXN["nullable"])


@pytest.mark.parametrize(
    "column, bad_value, type_name",
    [
        ("posted_date", "03/01/2026", "date"),
        ("amount", "twelve", "decimal"),
        ("amount", "NaN", "decimal"),
        ("is_planted_anomaly", "yes", "bool"),
    ],
)
def test_check_types_rejects_unparseable_values(column, bad_value, type_name):
    with pytest.raises(ValidationError, match=rf"transactions\.{column}: '{bad_value}' at row 0 is not a valid {type_name}"):
        check_types(good_frame(**{column: bad_value}), TXN["columns"], "transactions", TXN["nullable"])


def test_check_types_rejects_bad_int():
    with pytest.raises(ValidationError, match=r"t\.n: '1\.5' at row 0 is not a valid int"):
        check_types({"n": ["1.5"]}, {"n": "int"}, "t")


def test_check_types_rejects_null_in_non_nullable_column():
    with pytest.raises(ValidationError, match=r"transactions\.merchant: null at row 0, column is not nullable"):
        check_types(good_frame(merchant=None), TXN["columns"], "transactions", TXN["nullable"])


def test_check_types_treats_empty_string_and_nan_as_null():
    with pytest.raises(ValidationError, match="null at row 0"):
        check_types({"d": [""]}, {"d": "date"}, "t")
    with pytest.raises(ValidationError, match="null at row 0"):
        check_types({"d": [float("nan")]}, {"d": "date"}, "t")


def test_check_types_allows_null_in_nullable_column():
    check_types(good_frame(user_category=None), TXN["columns"], "transactions", ("user_category",))


def test_check_types_rejects_absent_column_and_unknown_type():
    with pytest.raises(ValidationError, match="column not present"):
        check_types({"a": ["x"]}, {"b": "string"}, "t")
    with pytest.raises(ValidationError, match="unknown expected type 'money'"):
        check_types({"a": ["x"]}, {"a": "money"}, "t")


def test_check_signed_amounts_accepts_mixed_signs():
    check_signed_amounts(["-1.00", "2.50", -3], "transactions")


def test_check_signed_amounts_rejects_unsigned_feed():
    with pytest.raises(ValidationError, match="all 3 amounts are positive; the feed looks unsigned"):
        check_signed_amounts(["1.00", "2.00", "3.00"], "transactions")


def test_check_signed_amounts_rejects_zero():
    with pytest.raises(ValidationError, match="zero at row 1"):
        check_signed_amounts(["-1.00", "0.00"], "transactions")


def test_check_signed_amounts_rejects_null_and_text():
    with pytest.raises(ValidationError, match="null at row 0"):
        check_signed_amounts([None, "-1"], "transactions")
    with pytest.raises(ValidationError, match="'abc' at row 0 is not numeric"):
        check_signed_amounts(["abc"], "transactions")


def test_check_signed_amounts_rejects_empty_head():
    with pytest.raises(ValidationError, match="no rows to validate"):
        check_signed_amounts(pd.Series([], dtype=object), "transactions")
