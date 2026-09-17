"""Gold: monthly spend, cash flow, daily net worth, anomalies and anomaly recall.

Transfer-flagged rows are excluded from every spending figure. Outflows are
reported as positive magnitudes; net worth stays signed.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from ducat_lakehouse.config import PipelineConfig, job_args, load_pipeline_config
from ducat_lakehouse.tables import write_table

_ZERO = F.lit(0).cast("decimal(18,2)")


def monthly_spend_by_category(txns: DataFrame) -> DataFrame:
    return (
        txns.filter(F.col("flow") == "outflow")
        .withColumn("month", F.date_format("posted_date", "yyyy-MM"))
        .groupBy("user_id", "month", "category")
        .agg((-F.sum("amount")).alias("spend"), F.count("*").alias("transactions"))
    )


def cash_flow_monthly(txns: DataFrame) -> DataFrame:
    return (
        txns.filter(F.col("flow") != "transfer")
        .withColumn("month", F.date_format("posted_date", "yyyy-MM"))
        .groupBy("user_id", "month")
        .agg(
            F.sum(F.when(F.col("amount") > 0, F.col("amount")).otherwise(_ZERO)).alias("inflow"),
            (-F.sum(F.when(F.col("amount") < 0, F.col("amount")).otherwise(_ZERO))).alias("outflow"),
        )
        .withColumn("net", F.col("inflow") - F.col("outflow"))
    )


def net_worth_daily(history: DataFrame) -> DataFrame:
    """Expand each SCD2 version over the days it was valid; keep days where every account is known."""
    accounts = history.groupBy("user_id").agg(F.countDistinct("account_id").alias("account_count"))
    last_day = F.coalesce(F.date_sub("valid_to", 1), F.col("last_observed_date"))
    return (
        history.withColumn("as_of_date", F.explode(F.sequence(F.col("valid_from"), last_day)))
        .groupBy("user_id", "as_of_date")
        .agg(F.sum("balance").alias("net_worth"), F.count("*").alias("accounts_known"))
        .join(accounts, "user_id")
        .filter(F.col("accounts_known") == F.col("account_count"))
        .select("user_id", "as_of_date", "net_worth", "account_count")
    )


def score_anomalies(txns: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """z-score of each outflow against the same user and category over the trailing window, excluding its own day."""
    trailing = (
        Window.partitionBy("user_id", "category").orderBy("_day").rangeBetween(-cfg.anomaly_window_days, -1)
    )
    z_score = (F.col("magnitude") - F.col("baseline_mean")) / F.col("baseline_stddev")
    return (
        txns.filter(F.col("flow") == "outflow")
        .withColumn("magnitude", (-F.col("amount")).cast("double"))
        .withColumn("_day", F.datediff(F.col("posted_date"), F.to_date(F.lit("1970-01-01"))))
        .withColumn("baseline_mean", F.avg("magnitude").over(trailing))
        .withColumn("baseline_stddev", F.stddev_samp("magnitude").over(trailing))
        .withColumn("baseline_count", F.count("magnitude").over(trailing))
        .withColumn(
            "z_score",
            F.when((F.col("baseline_count") >= cfg.anomaly_min_history) & (F.col("baseline_stddev") > 0), z_score),
        )
        .withColumn("is_anomaly", F.coalesce(F.col("z_score") > cfg.anomaly_z_threshold, F.lit(False)))
        .drop("_day")
    )


def anomaly_recall(scored: DataFrame, cfg: PipelineConfig) -> DataFrame:
    planted = F.col("is_planted_anomaly")
    flagged = F.col("is_anomaly")
    return (
        scored.agg(
            F.coalesce(F.sum(planted.cast("int")), F.lit(0)).alias("planted"),
            F.coalesce(F.sum((planted & flagged).cast("int")), F.lit(0)).alias("caught"),
            F.coalesce(F.sum(flagged.cast("int")), F.lit(0)).alias("flagged"),
        )
        .withColumn("recall", F.when(F.col("planted") > 0, F.col("caught") / F.col("planted")))
        .withColumn("precision", F.when(F.col("flagged") > 0, F.col("caught") / F.col("flagged")))
        .withColumn("z_threshold", F.lit(cfg.anomaly_z_threshold))
        .withColumn("window_days", F.lit(cfg.anomaly_window_days))
        .withColumn("computed_at", F.current_timestamp())
    )


def main(argv=None) -> None:
    args = job_args(argv)
    cfg = load_pipeline_config(catalog=args.catalog, schema=args.schema)
    spark = SparkSession.builder.getOrCreate()
    txns = spark.table(cfg.table("silver_transactions"))
    history = spark.table(cfg.table("silver_account_balance_history"))

    write_table(monthly_spend_by_category(txns), cfg.table("gold_monthly_spend_by_category"))
    write_table(cash_flow_monthly(txns), cfg.table("gold_cash_flow_monthly"))
    write_table(net_worth_daily(history), cfg.table("gold_net_worth_daily"))

    scored = score_anomalies(txns, cfg)
    anomalies = scored.filter("is_anomaly").select(
        "user_id", "account_id", "transaction_id", "posted_date", "merchant", "category", "amount", "magnitude",
        "baseline_mean", "baseline_stddev", "baseline_count", "z_score",
    )
    write_table(anomalies, cfg.table("gold_anomalies"))
    write_table(anomaly_recall(scored, cfg), cfg.table("gold_anomaly_recall"))
    spark.table(cfg.table("gold_anomaly_recall")).show(truncate=False)


if __name__ == "__main__":
    main()
