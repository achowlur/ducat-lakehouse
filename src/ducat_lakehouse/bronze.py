"""Bronze: raw CSVs from the volume into Delta, validated before anything is written."""

from __future__ import annotations

from pyspark.sql import SparkSession, functions as F

from ducat_lakehouse.config import job_args, load_pipeline_config
from ducat_lakehouse.rules import check_columns, check_signed_amounts, check_types
from ducat_lakehouse.schemas import SPARK_TYPES, TABLES
from ducat_lakehouse.tables import write_table

HEAD_ROWS = 1000


def ingest(spark: SparkSession, volume_path: str, table: str, target: str) -> int:
    contract = TABLES[table]
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.csv")
        .csv(f"{volume_path.rstrip('/')}/{table}")
    )

    check_columns(raw.columns, contract["columns"], table)
    head = raw.limit(HEAD_ROWS).toPandas()
    check_types(head, contract["columns"], table, contract["nullable"])
    if table == "transactions":
        check_signed_amounts(head["amount"], table)

    typed = raw.select(
        *[F.col(column).cast(SPARK_TYPES[kind]).alias(column) for column, kind in contract["columns"].items()],
        F.current_timestamp().alias("ingest_ts"),
        F.col("_metadata.file_path").alias("source_file"),
    )
    return write_table(typed, target, raw.count())


def main(argv=None) -> None:
    args = job_args(argv, volume_path=True)
    cfg = load_pipeline_config(catalog=args.catalog, schema=args.schema)
    spark = SparkSession.builder.getOrCreate()
    for table in TABLES:
        ingest(spark, args.volume_path, table, cfg.table(f"bronze_{table}"))


if __name__ == "__main__":
    main()
