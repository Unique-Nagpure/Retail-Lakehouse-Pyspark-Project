# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer: Cleansing, Dedup, SCD Type 2
# MAGIC
# MAGIC Two things happen in this notebook:
# MAGIC 1. **Orders**: clean the bronze orders (type casting, dropping bad rows,
# MAGIC    deduping late-arriving retries) into `silver.orders`.
# MAGIC 2. **Customers**: apply an **SCD Type 2** merge into `silver.dim_customer`
# MAGIC    so we retain full history of customer attribute changes (e.g. a
# MAGIC    customer's city changing) rather than overwriting in place.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/retail-lakehouse-pyspark/src")  # adjust to your repo path

from pyspark.sql import functions as F
from transformations import clean_orders, dedupe_latest, add_audit_columns, detect_customer_changes

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md ## Orders: bronze -> silver

# COMMAND ----------

bronze_orders = spark.table(f"{catalog}.bronze.orders_bronze")

silver_orders = (
    bronze_orders
    .transform(clean_orders)
    .transform(lambda df: dedupe_latest(df, keys=["order_id"], order_col="_ingested_at"))
    .transform(lambda df: add_audit_columns(df, source_name="bronze.orders_bronze"))
)

(
    silver_orders.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.orders")
)

display(spark.table(f"{catalog}.silver.orders").limit(10))

# COMMAND ----------

# MAGIC %md ## Customers: SCD Type 2 merge into dim_customer
# MAGIC
# MAGIC `customers.csv` represents the existing dimension snapshot; `customers_update.csv`
# MAGIC simulates a new batch of customer records where some rows are new,
# MAGIC some changed (e.g. city updated), and some unchanged.

# COMMAND ----------

incoming = spark.read.option("header", "true").csv(
    "/Volumes/retail_lakehouse/landing/customers_update.csv"
)

# First run: dim_customer may not exist yet. Bootstrap it from the base file.
if not spark.catalog.tableExists(f"{catalog}.silver.dim_customer"):
    base = (
        spark.read.option("header", "true").csv("/Volumes/retail_lakehouse/landing/customers.csv")
        .withColumn("effective_date", F.current_date())
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    base.write.format("delta").saveAsTable(f"{catalog}.silver.dim_customer")

# COMMAND ----------

current_dim = spark.table(f"{catalog}.silver.dim_customer").filter("is_current = true")
tracked_cols = ["name", "email", "city"]

classified = detect_customer_changes(current_dim, incoming, key="customer_id", tracked_cols=tracked_cols)
classified.createOrReplaceTempView("customer_changes")

# COMMAND ----------

# MAGIC %md
# MAGIC Expire the old row for any `CHANGED` customer, then insert the new
# MAGIC current version for both `NEW` and `CHANGED` records — the standard
# MAGIC two-step SCD Type 2 pattern implemented with `MERGE INTO`.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_lakehouse.silver.dim_customer AS target
# MAGIC USING (SELECT * FROM customer_changes WHERE _change_type = 'CHANGED') AS src
# MAGIC ON target.customer_id = src.customer_id AND target.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.end_date = current_date(),
# MAGIC   target.is_current = false

# COMMAND ----------

new_and_changed = classified.filter(F.col("_change_type").isin("NEW", "CHANGED")).select(
    "customer_id", "name", "email", "city"
).withColumn("effective_date", F.current_date()) \
 .withColumn("end_date", F.lit(None).cast("date")) \
 .withColumn("is_current", F.lit(True))

new_and_changed.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.silver.dim_customer"
)

display(spark.table(f"{catalog}.silver.dim_customer").orderBy("customer_id", "effective_date"))
