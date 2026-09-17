"""Deterministic synthetic accounts, transactions and balance snapshots.

Every user draws from its own random.Random seeded by "<seed>:<user index>",
so output depends only on (seed, user index, months, start month): the same
seed reproduces the same rows, and a user's rows do not depend on how many
other users are generated. Amounts are handled as integer cents throughout.
All names are invented.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import random
import shutil
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from ducat_lakehouse.schemas import TABLES

DEFAULT_START_MONTH = "2024-09"
BUCKETS = 16
PLANT_RATE = 0.008
DUPLICATE_RATE = 0.002

BANKS = ("Harbor Bank", "Summit Credit Union", "Prairie Savings")
CARD_ISSUERS = ("Pioneer Card Services", "Lakeside Card Co", "Beacon Card")
EMPLOYERS = ("Northwind", "Bluepeak Labs", "Cedar Logistics")
LANDLORDS = ("Oakline Property Mgmt", "Riverside Apartments")
SUBSCRIPTIONS = (
    ("Streamly", 1549),
    ("Tunewave", 1099),
    ("CloudVault", 299),
    ("FitHub Gym", 3900),
    ("NewsDaily", 800),
    ("GameArc", 1499),
    ("Readly Books", 1199),
    ("Podnest Premium", 599),
)
GROCERY_MERCHANTS = ("Greenleaf Market", "Harvest Grocer", "Corner Pantry")
DINING_MERCHANTS = ("Blue Door Cafe", "Luna Noodle Bar", "Sprout Salads", "Ember Pizza")
ANOMALY_ELIGIBLE = frozenset(GROCERY_MERCHANTS + DINING_MERCHANTS)


def _months(start_month: str, months: int):
    year, month = (int(part) for part in start_month.split("-"))
    for offset in range(months):
        carry, index = divmod(month - 1 + offset, 12)
        yield year + carry, index + 1


def _money(cents: int) -> Decimal:
    return Decimal(cents).scaleb(-2)


def generate_user(
    seed: int,
    user_index: int,
    months: int,
    start_month: str = DEFAULT_START_MONTH,
    duplicate_rate: float = DUPLICATE_RATE,
) -> dict[str, list[dict]]:
    rng = random.Random(f"{seed}:{user_index}")
    user_id = f"u{user_index:06d}"
    periods = list(_months(start_month, months))
    first_day = date(periods[0][0], periods[0][1], 1)

    bank = rng.choice(BANKS)
    issuer = rng.choice(CARD_ISSUERS)
    ids = {"checking": f"{user_id}-chk", "savings": f"{user_id}-sav", "card": f"{user_id}-crd"}
    institutions = {"checking": bank, "savings": bank, "card": issuer}
    accounts = [
        {
            "user_id": user_id,
            "account_id": ids[kind],
            "account_type": kind,
            "institution": institutions[kind],
            "opened_date": first_day - timedelta(days=rng.randint(30, 3000)),
        }
        for kind in ("checking", "savings", "card")
    ]
    opening = {
        ids["checking"]: rng.randint(1500_00, 6000_00),
        ids["savings"]: rng.randint(2000_00, 25000_00),
        ids["card"]: 0,
    }

    employer = rng.choice(EMPLOYERS)
    paycheck = rng.randint(1800_00, 4200_00)
    landlord = rng.choice(LANDLORDS)
    rent = rng.randint(1100_00, 2600_00)
    subscriptions = rng.sample(SUBSCRIPTIONS, rng.randint(3, 6))
    subscription_days = [rng.randint(1, 28) for _ in subscriptions]
    grocery_mean = rng.randint(70_00, 160_00)
    dining_mean = rng.randint(18_00, 45_00)
    grocery_offset = rng.randint(0, 6)

    raw: list[dict] = []

    def add(day, account_kind, cents, merchant, description, planted=False, user_category=None):
        raw.append(
            {
                "posted_date": day,
                "account_id": ids[account_kind],
                "cents": cents,
                "merchant": merchant,
                "description": description,
                "is_planted_anomaly": planted,
                "user_category": user_category,
            }
        )

    def spend(day, merchants, mean):
        merchant = rng.choice(merchants)
        cents = max(150, int(rng.gauss(mean, mean * 0.25)))
        planted = rng.random() < PLANT_RATE
        if planted:
            cents = int(mean * rng.uniform(5, 10))
        user_category = None
        if not planted:
            if merchant == "Corner Pantry" and rng.random() < 0.5:
                user_category = "Groceries"
            elif merchant in DINING_MERCHANTS and rng.random() < 0.02:
                user_category = "Travel"
        add(day, "card", -cents, merchant, f"{merchant.upper()} CARD PURCHASE", planted, user_category)

    card_due = 0
    for year, month in periods:
        last = calendar.monthrange(year, month)[1]
        month_start = len(raw)

        for payday in (1, 15):
            add(date(year, month, payday), "checking", paycheck, f"{employer} Payroll",
                f"{employer.upper()} PAYROLL DIRECT DEP")
        add(date(year, month, 1), "checking", -rent, landlord, f"{landlord.upper()} RENT")
        for (merchant, cents), day in zip(subscriptions, subscription_days):
            add(date(year, month, min(day, last)), "card", -cents, merchant, f"{merchant.upper()} RECURRING")

        for week_start in range(1, last + 1, 7):
            grocery_day = week_start + grocery_offset
            if grocery_day <= last:
                spend(date(year, month, grocery_day), GROCERY_MERCHANTS, grocery_mean)
            for _ in range(rng.randint(1, 3)):
                spend(date(year, month, rng.randint(week_start, min(week_start + 6, last))), DINING_MERCHANTS,
                      dining_mean)

        for _ in range(rng.randint(1, 2)):
            cents = rng.randint(150_00, 900_00)
            day = rng.randint(2, 24)
            lag = rng.randint(0, 4)
            add(date(year, month, day), "checking", -cents, bank, "TRANSFER TO SAVINGS")
            add(date(year, month, day + lag), "savings", cents, bank, "TRANSFER FROM CHECKING")

        if card_due:
            lag = rng.randint(0, 3)
            add(date(year, month, 5), "checking", -card_due, issuer, f"{issuer.upper()} AUTOPAY PAYMENT")
            add(date(year, month, 5 + lag), "card", card_due, issuer, "PAYMENT RECEIVED THANK YOU")
        card_due = -sum(row["cents"] for row in raw[month_start:] if row["account_id"] == ids["card"]
                        and row["cents"] < 0)

    raw.sort(key=lambda row: (row["posted_date"], row["account_id"], row["description"], row["cents"]))

    transactions = []
    for row in raw:
        copies = 2 if rng.random() < duplicate_rate else 1
        for _ in range(copies):
            transactions.append(
                {
                    "user_id": user_id,
                    "account_id": row["account_id"],
                    "transaction_id": f"{user_id}-{len(transactions):06d}",
                    "posted_date": row["posted_date"],
                    "amount": _money(row["cents"]),
                    "merchant": row["merchant"],
                    "description": row["description"],
                    "user_category": row["user_category"],
                    "is_planted_anomaly": row["is_planted_anomaly"],
                }
            )

    snapshots = []
    for account_id, opening_cents in opening.items():
        balance = opening_cents
        pending = iter(r for r in raw if r["account_id"] == account_id)
        next_row = next(pending, None)
        for year, month in periods:
            month_end = date(year, month, calendar.monthrange(year, month)[1])
            while next_row is not None and next_row["posted_date"] <= month_end:
                balance += next_row["cents"]
                next_row = next(pending, None)
            snapshots.append(
                {"user_id": user_id, "account_id": account_id, "snapshot_date": month_end, "balance": _money(balance)}
            )

    return {"accounts": accounts, "transactions": transactions, "balance_snapshots": snapshots}


def _format(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _writer(path: Path, table: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "w", encoding="utf-8", newline="")
    writer = csv.writer(handle, lineterminator="\n")
    writer.writerow(TABLES[table]["columns"])
    return handle, writer


def _write_rows(writer, table: str, rows: list[dict]) -> None:
    columns = TABLES[table]["columns"]
    writer.writerows([_format(row[column]) for column in columns] for row in rows)


def write_dataset(
    out_dir: str | Path,
    users: int,
    months: int,
    seed: int,
    start_month: str = DEFAULT_START_MONTH,
    buckets: int = BUCKETS,
) -> dict[str, int]:
    """Write <out_dir>/<table>/bucket=NN/part-00000.csv, replacing earlier output."""
    out = Path(out_dir)
    for table in TABLES:
        shutil.rmtree(out / table, ignore_errors=True)
    handles = {}
    counts = dict.fromkeys(TABLES, 0)
    try:
        for user_index in range(users):
            bucket = f"bucket={user_index % buckets:02d}"
            for table, rows in generate_user(seed, user_index, months, start_month).items():
                key = (table, bucket)
                if key not in handles:
                    handles[key] = _writer(out / table / bucket / "part-00000.csv", table)
                _write_rows(handles[key][1], table, rows)
                counts[table] += len(rows)
    finally:
        for handle, _ in handles.values():
            handle.close()
    return counts


def write_sample(out_dir: str | Path | None = None, users: int = 5, months: int = 3, seed: int = 42) -> dict[str, int]:
    """Write the small committed sample as flat CSVs under sample_data/."""
    out = Path(out_dir) if out_dir else Path(__file__).resolve().parents[2] / "sample_data"
    counts = {}
    generated = [generate_user(seed, index, months) for index in range(users)]
    for table in TABLES:
        handle, writer = _writer(out / f"{table}.csv", table)
        with handle:
            rows = [row for user in generated for row in user[table]]
            _write_rows(writer, table, rows)
            counts[table] = len(rows)
    return counts


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic Ducat data")
    parser.add_argument("--volume-path")
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--months", type=int, default=24)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-month", default=DEFAULT_START_MONTH)
    parser.add_argument("--sample", action="store_true", help="write the committed sample under sample_data/")
    args = parser.parse_args(argv)

    if args.sample:
        print(write_sample(seed=args.seed))
        return
    if not args.volume_path or not args.volume_path.startswith("/Volumes/"):
        raise SystemExit("--volume-path must be a Unity Catalog volume path starting with /Volumes/")
    if args.users < 1 or args.months < 1:
        raise SystemExit("--users and --months must be positive")
    counts = write_dataset(args.volume_path, args.users, args.months, args.seed, args.start_month)
    print(f"wrote {counts} to {args.volume_path}")


if __name__ == "__main__":
    main()
