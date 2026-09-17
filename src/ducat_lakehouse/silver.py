"""Silver: deduplicated, categorized, transfer-paired transactions and SCD2 balance history.

Rule matching and transfer pairing mirror rules.apply_rules and
rules.pair_transfers, which the tests pin.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from ducat_lakehouse.config import PipelineConfig, job_args, load_pipeline_config, load_rules
from ducat_lakehouse.rules import UNCATEGORIZED, Rule
from ducat_lakehouse.tables import write_table

DEDUP_KEY = ["user_id", "account_id", "posted_date", "amount", "description"]

_FIELD_VALUE = "coalesce(CASE rule_field WHEN 'merchant' THEN merchant ELSE description END, '')"
_RULE_MATCH = f"""
CASE rule_operator
  WHEN 'contains' THEN instr(lower({_FIELD_VALUE}), lower(rule_value)) > 0
  WHEN 'equals' THEN lower({_FIELD_VALUE}) = lower(rule_value)
  WHEN 'starts_with' THEN substring(lower({_FIELD_VALUE}), 1, length(rule_value)) = lower(rule_value)
  WHEN 'regex' THEN {_FIELD_VALUE} RLIKE concat('(?i)', rule_value)
  ELSE false
END
"""


def dedup_transactions(txns: DataFrame) -> DataFrame:
    first = Window.partitionBy(*DEDUP_KEY).orderBy("transaction_id")
    return txns.withColumn("_copy", F.row_number().over(first)).filter("_copy = 1").drop("_copy")


def rules_frame(spark: SparkSession, rules: list[Rule]) -> DataFrame:
    return spark.createDataFrame(
        [(r.priority, r.field, r.operator, r.value, r.category) for r in rules],
        "rule_priority int, rule_field string, rule_operator string, rule_value string, rule_category string",
    )


def categorize(txns: DataFrame, rules: DataFrame) -> DataFrame:
    first_match = Window.partitionBy("transaction_id").orderBy("rule_priority")
    matches = (
        txns.select("transaction_id", "merchant", "description")
        .join(F.broadcast(rules), F.expr(_RULE_MATCH), "inner")
        .withColumn("_rank", F.row_number().over(first_match))
        .filter("_rank = 1")
        .select("transaction_id", "rule_category")
    )
    return (
        txns.join(matches, "transaction_id", "left")
        .withColumn("category", F.coalesce("user_category", "rule_category", F.lit(UNCATEGORIZED)))
        .withColumn(
            "category_source",
            F.when(F.col("user_category").isNotNull(), "user")
            .when(F.col("rule_category").isNotNull(), "rule")
            .otherwise("none"),
        )
        .drop("rule_category")
    )


def pair_transfers(txns: DataFrame, window_days: int) -> DataFrame:
    columns = ["transaction_id", "user_id", "account_id", "posted_date", "amount"]
    eligible = txns.filter(F.col("user_category").isNull()).select(*columns)
    outs = eligible.filter("amount < 0").select(*[F.col(c).alias(f"out_{c}") for c in columns])
    ins = eligible.filter("amount > 0").select(*[F.col(c).alias(f"in_{c}") for c in columns])
    gap = F.abs(F.datediff("in_posted_date", "out_posted_date"))

    candidates = outs.join(
        ins,
        (F.col("out_user_id") == F.col("in_user_id"))
        & (F.col("out_account_id") != F.col("in_account_id"))
        & (F.col("in_amount") == -F.col("out_amount"))
        & (gap <= window_days),
    ).select(F.col("out_transaction_id").alias("out_id"), F.col("in_transaction_id").alias("in_id"), gap.alias("gap"))

    pairs = (
        candidates.withColumn("_out_rank", F.row_number().over(Window.partitionBy("out_id").orderBy("gap", "in_id")))
        .withColumn("_in_rank", F.row_number().over(Window.partitionBy("in_id").orderBy("gap", "out_id")))
        .filter("_out_rank = 1 AND _in_rank = 1")
    )
    partners = pairs.select(
        F.col("out_id").alias("transaction_id"), F.col("in_id").alias("transfer_partner_id")
    ).unionByName(pairs.select(F.col("in_id").alias("transaction_id"), F.col("out_id").alias("transfer_partner_id")))

    return txns.join(partners, "transaction_id", "left").withColumn(
        "flow",
        F.when(F.col("transfer_partner_id").isNotNull(), "transfer")
        .when(F.col("amount") > 0, "inflow")
        .otherwise("outflow"),
    )


def build_transactions(spark: SparkSession, cfg: PipelineConfig, rules: list[Rule]) -> int:
    source = spark.table(cfg.table("bronze_transactions")).drop("ingest_ts", "source_file")
    deduped = dedup_transactions(source)
    expected = deduped.count()
    enriched = pair_transfers(categorize(deduped, rules_frame(spark, rules)), cfg.transfer_window_days)
    silver = enriched.select(
        "user_id", "account_id", "transaction_id", "posted_date", "amount", "merchant", "description",
        "user_category", "category", "category_source", "flow", "transfer_partner_id", "is_planted_anomaly",
    )
    return write_table(silver, cfg.table("silver_transactions"), expected)


def balance_history(snapshots: DataFrame) -> DataFrame:
    """SCD Type 2: one version per run of unchanged balances; valid_to is exclusive."""
    account = Window.partitionBy("user_id", "account_id").orderBy("snapshot_date")
    versions = (
        snapshots.withColumn("_previous", F.lag("balance").over(account))
        .withColumn("_changed", F.when(F.col("_previous").isNull() | (F.col("_previous") != F.col("balance")), 1)
                    .otherwise(0))
        .withColumn("_version", F.sum("_changed").over(account.rowsBetween(Window.unboundedPreceding, 0)))
        .groupBy("user_id", "account_id", "_version")
        .agg(
            F.min("snapshot_date").alias("valid_from"),
            F.max("snapshot_date").alias("last_observed_date"),
            F.first("balance").alias("balance"),
        )
    )
    ordered = Window.partitionBy("user_id", "account_id").orderBy("valid_from")
    return (
        versions.withColumn("valid_to", F.lead("valid_from").over(ordered))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn(
            "balance_sk",
            F.sha2(F.concat_ws("|", "user_id", "account_id", F.col("valid_from").cast("string")), 256),
        )
        .select("balance_sk", "user_id", "account_id", "balance", "valid_from", "valid_to", "is_current",
                "last_observed_date")
    )


def build_balance_history(spark: SparkSession, cfg: PipelineConfig) -> int:
    latest = Window.partitionBy("user_id", "account_id", "snapshot_date").orderBy(F.col("ingest_ts").desc())
    snapshots = (
        spark.table(cfg.table("bronze_balance_snapshots"))
        .withColumn("_rank", F.row_number().over(latest))
        .filter("_rank = 1")
        .select("user_id", "account_id", "snapshot_date", "balance")
    )
    history = balance_history(snapshots)
    return write_table(history, cfg.table("silver_account_balance_history"), history.count())


def main(argv=None) -> None:
    args = job_args(argv)
    cfg = load_pipeline_config(catalog=args.catalog, schema=args.schema)
    spark = SparkSession.builder.getOrCreate()
    build_transactions(spark, cfg, load_rules())
    build_balance_history(spark, cfg)


if __name__ == "__main__":
    main()
