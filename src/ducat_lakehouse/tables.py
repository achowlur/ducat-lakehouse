"""Delta table writes with a row-count check."""

from __future__ import annotations


class RowCountMismatch(RuntimeError):
    pass


def write_table(df, name: str, expected_rows: int | None = None) -> int:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    actual = df.sparkSession.table(name).count()
    if expected_rows is not None and actual != expected_rows:
        raise RowCountMismatch(f"{name}: expected {expected_rows} rows from source, table has {actual}")
    print(f"{name}: {actual} rows")
    return actual
