"""
transformations.py
-------------------
Reusable PySpark transformation functions for the Retail Lakehouse pipeline.

Keeping this logic outside the notebooks makes it independently unit-testable
(see tests/test_transformations.py) and reusable across bronze/silver/gold
layers instead of copy-pasted inline in each notebook.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedupe_latest(df: DataFrame, keys: list, order_col: str) -> DataFrame:
    """
    Keep only the latest record per key group, based on order_col descending.
    Used to drop duplicate order/customer events that may arrive from
    upstream retries or Auto Loader re-processing.
    """
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    """Attach standard audit/lineage columns used across all silver tables."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_name))
    )


def clean_orders(df: DataFrame) -> DataFrame:
    """
    Apply core data-quality rules to raw order records:
      - drop rows missing required keys
      - cast quantity to int and guard against non-positive values
      - normalize status to uppercase
    """
    return (
        df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn("quantity", F.col("quantity").cast("int"))
        .filter(F.col("quantity") > 0)
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
    )


def build_order_facts(orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Join cleaned orders with product catalog to compute line-level revenue.
    This is the core fact-building transformation feeding the gold layer.
    """
    return (
        orders_df.join(products_df, on="product_id", how="left")
        .withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("order_date", F.to_date("order_ts"))
    )


def daily_sales_summary(facts_df: DataFrame) -> DataFrame:
    """
    Aggregate order facts into a daily sales summary (gold layer):
    total revenue, order count, and units sold per day and category.
    Excludes cancelled/returned orders from revenue.
    """
    valid_orders = facts_df.filter(~F.col("status").isin("CANCELLED", "RETURNED"))
    return (
        valid_orders.groupBy("order_date", "category")
        .agg(
            F.sum("line_amount").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy("order_date", "category")
    )


def detect_customer_changes(existing_df: DataFrame, incoming_df: DataFrame, key: str, tracked_cols: list):
    """
    Compare incoming customer records against the current dimension snapshot
    to classify rows for an SCD Type 2 merge:
      - NEW: key not present in existing_df
      - CHANGED: key present but one or more tracked columns differ
      - UNCHANGED: key present and all tracked columns match

    Returns the incoming_df with an added `_change_type` column.
    Notebook 02 uses this output to drive a MERGE INTO for SCD Type 2.
    """
    existing_aliased = existing_df.select(
        key, *[F.col(c).alias(f"_existing_{c}") for c in tracked_cols]
    )

    joined = incoming_df.join(existing_aliased, on=key, how="left")

    change_condition = F.lit(False)
    for c in tracked_cols:
        change_condition = change_condition | (F.col(c) != F.col(f"_existing_{c}"))

    result = joined.withColumn(
        "_change_type",
        F.when(F.col(f"_existing_{tracked_cols[0]}").isNull(), "NEW")
        .when(change_condition, "CHANGED")
        .otherwise("UNCHANGED"),
    )

    return result.drop(*[f"_existing_{c}" for c in tracked_cols])
