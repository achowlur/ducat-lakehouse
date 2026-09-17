"""Column contracts for the three raw CSV sets, shared by the generator and bronze."""

TABLES = {
    "accounts": {
        "columns": {
            "user_id": "string",
            "account_id": "string",
            "account_type": "string",
            "institution": "string",
            "opened_date": "date",
        },
        "nullable": (),
    },
    "transactions": {
        "columns": {
            "user_id": "string",
            "account_id": "string",
            "transaction_id": "string",
            "posted_date": "date",
            "amount": "decimal",
            "merchant": "string",
            "description": "string",
            "user_category": "string",
            "is_planted_anomaly": "bool",
        },
        "nullable": ("user_category",),
    },
    "balance_snapshots": {
        "columns": {
            "user_id": "string",
            "account_id": "string",
            "snapshot_date": "date",
            "balance": "decimal",
        },
        "nullable": (),
    },
}

SPARK_TYPES = {
    "string": "string",
    "date": "date",
    "decimal": "decimal(18,2)",
    "bool": "boolean",
    "int": "int",
}
