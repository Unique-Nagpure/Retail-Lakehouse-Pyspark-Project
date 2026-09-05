"""
Unit tests for src/transformations.py.

Run locally with: pytest tests/
(requires `pip install pyspark pytest` — no Databricks cluster needed,
since these functions are pure PySpark DataFrame transformations.)
"""

import pytest
from pyspark.sql import SparkSession

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from transformations import (
    dedupe_latest,
    clean_orders,
    build_order_facts,
    daily_sales_summary,
    detect_customer_changes,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("transformations-tests")
        .getOrCreate()
    )


def test_dedupe_latest_keeps_most_recent(spark):
    df = spark.createDataFrame(
        [("O1", "2024-01-01T10:00:00"), ("O1", "2024-01-01T12:00:00")],
        ["order_id", "_ingested_at"],
    )
    result = dedupe_latest(df, keys=["order_id"], order_col="_ingested_at")
    assert result.count() == 1
    assert result.collect()[0]["_ingested_at"] == "2024-01-01T12:00:00"


def test_clean_orders_drops_invalid_rows(spark):
    df = spark.createDataFrame(
        [
            ("O1", "C1", 2, "2024-01-01T10:00:00", "completed"),
            (None, "C2", 1, "2024-01-01T10:00:00", "completed"),
            ("O3", "C3", 0, "2024-01-01T10:00:00", "completed"),
        ],
        ["order_id", "customer_id", "quantity", "order_ts", "status"],
    )
    result = clean_orders(df)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["order_id"] == "O1"
    assert rows[0]["status"] == "COMPLETED"


def test_build_order_facts_computes_line_amount(spark):
    from pyspark.sql import functions as F

    orders = spark.createDataFrame(
        [("O1", "P1", 3, "2024-01-01T10:00:00", "COMPLETED")],
        ["order_id", "product_id", "quantity", "order_ts", "status"],
    ).withColumn("order_ts", F.to_timestamp("order_ts"))
    products = spark.createDataFrame([("P1", "Widget", "Misc", 10.0)], ["product_id", "product_name", "category", "unit_price"])

    result = build_order_facts(orders, products)
    row = result.collect()[0]
    assert row["line_amount"] == 30.0


def test_daily_sales_summary_excludes_cancelled(spark):
    from pyspark.sql import functions as F

    facts = spark.createDataFrame(
        [
            ("2024-01-01", "Misc", "O1", 100.0, 2, "COMPLETED"),
            ("2024-01-01", "Misc", "O2", 50.0, 1, "CANCELLED"),
        ],
        ["order_date", "category", "order_id", "line_amount", "quantity", "status"],
    ).withColumn("order_date", F.to_date("order_date"))

    result = daily_sales_summary(facts)
    row = result.collect()[0]
    assert row["total_revenue"] == 100.0
    assert row["order_count"] == 1


def test_detect_customer_changes_classifies_correctly(spark):
    existing = spark.createDataFrame(
        [("C1", "Alice", "a@x.com", "Pune"), ("C2", "Bob", "b@x.com", "Delhi")],
        ["customer_id", "name", "email", "city"],
    )
    incoming = spark.createDataFrame(
        [
            ("C1", "Alice", "a@x.com", "Mumbai"),  # changed city
            ("C2", "Bob", "b@x.com", "Delhi"),      # unchanged
            ("C3", "Cara", "c@x.com", "Chennai"),   # new
        ],
        ["customer_id", "name", "email", "city"],
    )
    result = detect_customer_changes(existing, incoming, key="customer_id", tracked_cols=["name", "email", "city"])
    changes = {row["customer_id"]: row["_change_type"] for row in result.collect()}
    assert changes == {"C1": "CHANGED", "C2": "UNCHANGED", "C3": "NEW"}
