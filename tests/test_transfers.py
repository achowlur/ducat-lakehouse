from ducat_lakehouse.rules import pair_transfers


def txn(txn_id, account, day, amount, user="u1", user_category=None):
    return {
        "transaction_id": txn_id,
        "user_id": user,
        "account_id": account,
        "posted_date": f"2026-03-{day:02d}",
        "amount": amount,
        "user_category": user_category,
    }


def test_opposite_amounts_across_accounts_pair_both_ways():
    rows = [txn("a", "chk", 3, "-250.00"), txn("b", "sav", 5, "250.00")]
    assert pair_transfers(rows) == {"a": "b", "b": "a"}


def test_window_is_inclusive_at_four_days():
    assert pair_transfers([txn("a", "chk", 3, "-250.00"), txn("b", "sav", 7, "250.00")]) == {"a": "b", "b": "a"}
    assert pair_transfers([txn("a", "chk", 3, "-250.00"), txn("b", "sav", 8, "250.00")]) == {}


def test_inflow_may_precede_outflow():
    assert pair_transfers([txn("a", "chk", 6, "-40.00"), txn("b", "sav", 4, "40.00")]) == {"a": "b", "b": "a"}


def test_same_account_does_not_pair():
    assert pair_transfers([txn("a", "chk", 3, "-250.00"), txn("b", "chk", 3, "250.00")]) == {}


def test_different_users_do_not_pair():
    assert pair_transfers([txn("a", "chk", 3, "-250.00", user="u1"), txn("b", "sav", 3, "250.00", user="u2")]) == {}


def test_amounts_must_be_exactly_opposite():
    assert pair_transfers([txn("a", "chk", 3, "-250.00"), txn("b", "sav", 3, "250.01")]) == {}
    assert pair_transfers([txn("a", "chk", 3, "-250.00"), txn("b", "sav", 3, "-250.00")]) == {}


def test_amount_representations_compare_by_value():
    assert pair_transfers([txn("a", "chk", 3, -250), txn("b", "sav", 3, "250.00")]) == {"a": "b", "b": "a"}


def test_user_categorized_rows_are_never_paired():
    rows = [txn("a", "chk", 3, "-250.00", user_category="Rent"), txn("b", "sav", 3, "250.00")]
    assert pair_transfers(rows) == {}


def test_closest_candidate_wins_and_pairing_is_one_to_one():
    rows = [
        txn("out", "chk", 10, "-99.00"),
        txn("far", "sav", 13, "99.00"),
        txn("near", "crd", 11, "99.00"),
    ]
    assert pair_transfers(rows) == {"out": "near", "near": "out"}


def test_ties_break_by_transaction_id_on_both_sides():
    rows = [
        txn("o2", "chk", 10, "-10.00"),
        txn("o1", "chk", 10, "-10.00"),
        txn("i2", "sav", 10, "10.00"),
        txn("i1", "sav", 10, "10.00"),
    ]
    pairs = pair_transfers(rows)
    assert pairs["o1"] == "i1" and pairs["i1"] == "o1"
    assert "o2" not in pairs and "i2" not in pairs


def test_pairs_only_when_the_choice_is_mutual():
    rows = [
        txn("o_close", "chk", 10, "-10.00"),
        txn("o_far", "chk", 7, "-10.00"),
        txn("i", "sav", 10, "10.00"),
    ]
    assert pair_transfers(rows) == {"o_close": "i", "i": "o_close"}


def test_custom_window():
    rows = [txn("a", "chk", 1, "-5.00"), txn("b", "sav", 9, "5.00")]
    assert pair_transfers(rows, window_days=4) == {}
    assert pair_transfers(rows, window_days=8) == {"a": "b", "b": "a"}
