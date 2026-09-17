import calendar
import csv
import statistics
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from ducat_lakehouse import synth
from ducat_lakehouse.config import load_rules
from ducat_lakehouse.rules import categorize, pair_transfers
from ducat_lakehouse.schemas import TABLES

REPO = Path(__file__).resolve().parents[1]


def generate(users=40, months=12, seed=42, **kwargs):
    return [synth.generate_user(seed, index, months, **kwargs) for index in range(users)]


def flatten(generated, table):
    return [row for user in generated for row in user[table]]


@pytest.fixture(scope="module")
def dataset():
    return generate()


def test_same_seed_same_rows():
    assert generate(users=5, months=6) == generate(users=5, months=6)


def test_different_seed_different_rows():
    assert generate(users=5, months=6, seed=1) != generate(users=5, months=6, seed=2)


def test_user_rows_do_not_depend_on_population_size():
    assert synth.generate_user(42, 3, 6) == generate(users=10, months=6)[3]


def test_written_dataset_is_byte_identical_across_runs(tmp_path):
    synth.write_dataset(tmp_path / "a", users=6, months=2, seed=7, buckets=4)
    synth.write_dataset(tmp_path / "b", users=6, months=2, seed=7, buckets=4)
    files_a = sorted(p.relative_to(tmp_path / "a") for p in (tmp_path / "a").rglob("*.csv"))
    files_b = sorted(p.relative_to(tmp_path / "b") for p in (tmp_path / "b").rglob("*.csv"))
    assert files_a == files_b
    assert Path("transactions/bucket=03/part-00000.csv") in files_a
    for relative in files_a:
        assert (tmp_path / "a" / relative).read_bytes() == (tmp_path / "b" / relative).read_bytes()


def test_rewrite_replaces_previous_output(tmp_path):
    synth.write_dataset(tmp_path, users=8, months=1, seed=1, buckets=8)
    synth.write_dataset(tmp_path, users=2, months=1, seed=1, buckets=8)
    assert len(list((tmp_path / "accounts").rglob("*.csv"))) == 2


def test_committed_sample_matches_generator(tmp_path):
    synth.write_sample(tmp_path)
    for table in TABLES:
        with open(REPO / "sample_data" / f"{table}.csv", newline="", encoding="utf-8") as committed, open(
            tmp_path / f"{table}.csv", newline="", encoding="utf-8"
        ) as fresh:
            assert list(csv.reader(committed)) == list(csv.reader(fresh)), table


def test_each_user_has_checking_savings_and_card(dataset):
    for user in dataset:
        assert sorted(a["account_type"] for a in user["accounts"]) == ["card", "checking", "savings"]


@pytest.fixture(scope="module")
def clean():
    return generate(duplicate_rate=0)


def is_transfer_leg(row):
    return (
        row["description"] in ("TRANSFER TO SAVINGS", "TRANSFER FROM CHECKING", "PAYMENT RECEIVED THANK YOU")
        or row["description"].endswith("AUTOPAY PAYMENT")
    )


def test_monthly_structure(clean):
    for user in clean:
        by_month = defaultdict(list)
        for row in user["transactions"]:
            by_month[row["posted_date"].strftime("%Y-%m")].append(row)
        assert len(by_month) == 12
        for rows in by_month.values():
            assert sum("PAYROLL" in r["description"] for r in rows) == 2
            assert sum(r["description"].endswith(" RENT") for r in rows) == 1
            assert 3 <= sum("RECURRING" in r["description"] for r in rows) <= 6
            assert 1 <= sum(r["description"] == "TRANSFER TO SAVINGS" for r in rows) <= 2
            assert sum(1 for r in rows if "CARD PURCHASE" in r["description"] and r["merchant"] in
                       synth.GROCERY_MERCHANTS) >= 4


def test_transfer_legs_pair_exactly_and_nothing_else_pairs(clean):
    rows = flatten(clean, "transactions")
    partners = pair_transfers(rows, window_days=4)
    legs = {r["transaction_id"] for r in rows if is_transfer_leg(r)}
    assert legs
    assert set(partners) == legs
    by_id = {r["transaction_id"]: r for r in rows}
    for txn_id, partner in partners.items():
        assert by_id[txn_id]["amount"] == -by_id[partner]["amount"]
        assert by_id[txn_id]["account_id"] != by_id[partner]["account_id"]
        assert abs((by_id[txn_id]["posted_date"] - by_id[partner]["posted_date"]).days) <= 4


def test_planted_anomalies_are_about_half_a_percent_of_outflows(dataset):
    outflows = [r for r in flatten(dataset, "transactions") if r["amount"] < 0]
    planted = [r for r in outflows if r["is_planted_anomaly"]]
    assert 0.002 <= len(planted) / len(outflows) <= 0.01


def test_planted_anomalies_are_labeled_only_on_inflated_everyday_outflows(dataset):
    for user in dataset:
        normal = defaultdict(list)
        for row in user["transactions"]:
            if row["amount"] < 0 and not row["is_planted_anomaly"]:
                normal[row["merchant"]].append(-row["amount"])
        for row in user["transactions"]:
            if not row["is_planted_anomaly"]:
                continue
            assert row["amount"] < 0
            assert row["merchant"] in synth.ANOMALY_ELIGIBLE
            assert row["user_category"] is None
            peers = [v for m, values in normal.items() if m in synth.ANOMALY_ELIGIBLE for v in values
                     if (m in synth.GROCERY_MERCHANTS) == (row["merchant"] in synth.GROCERY_MERCHANTS)]
            assert -row["amount"] >= 4 * statistics.median(peers)


def test_user_category_is_present_but_rare(dataset):
    rows = flatten(dataset, "transactions")
    labeled = [r for r in rows if r["user_category"] is not None]
    assert 0 < len(labeled) < 0.1 * len(rows)
    assert {r["user_category"] for r in labeled} <= {"Groceries", "Travel"}


def test_month_end_snapshots_follow_transactions():
    user = synth.generate_user(42, 0, 6, duplicate_rate=0)
    by_account = defaultdict(list)
    for snap in user["balance_snapshots"]:
        by_account[snap["account_id"]].append(snap)
    for account_id, snaps in by_account.items():
        assert all(
            s["snapshot_date"].day == calendar.monthrange(s["snapshot_date"].year, s["snapshot_date"].month)[1]
            for s in snaps
        )
        for earlier, later in zip(snaps, snaps[1:]):
            moved = sum(
                (t["amount"] for t in user["transactions"]
                 if t["account_id"] == account_id and earlier["snapshot_date"] < t["posted_date"] <= later["snapshot_date"]),
                Decimal("0"),
            )
            assert later["balance"] - earlier["balance"] == moved


def test_duplicates_are_exact_copies_with_new_ids(dataset):
    rows = flatten(dataset, "transactions")
    keys = defaultdict(list)
    for row in rows:
        keys[(row["user_id"], row["account_id"], row["posted_date"], row["amount"], row["description"])].append(row)
    assert len({r["transaction_id"] for r in rows}) == len(rows)
    assert any(len(group) > 1 for group in keys.values())


def test_generated_merchants_are_covered_by_shipped_rules(dataset):
    rules = load_rules()
    categories = {
        categorize(None, r["merchant"], r["description"], rules) for r in flatten(dataset, "transactions")
    }
    assert {"Income", "Rent", "Subscriptions", "Groceries", "Dining", "Transfer", "Uncategorized"} <= categories


def test_job_entry_refuses_paths_outside_volumes():
    with pytest.raises(SystemExit, match="/Volumes/"):
        synth.main(["--volume-path", str(REPO / "sample_data")])


def test_dates_stay_inside_the_requested_months(dataset):
    rows = flatten(dataset, "transactions")
    assert min(r["posted_date"] for r in rows) >= date(2024, 9, 1)
    assert max(r["posted_date"] for r in rows) <= date(2025, 8, 31)
