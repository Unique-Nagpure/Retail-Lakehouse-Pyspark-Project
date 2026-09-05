# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer: Sales Facts & Daily Aggregates
# MAGIC
# MAGIC Joins silver orders with the product catalog to build a line-level fact
# MAGIC table, then rolls it up into a daily sales summary by category — the
# MAGIC table a BI tool (e.g. Power BI) would connect to directly.

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/retail-lakehouse-pyspark/src")

from transformations import build_order_facts, daily_sales_summary

dbutils.widgets.text("catalog", "retail_lakehouse")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

silver_orders = spark.table(f"{catalog}.silver.orders")
products = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "/Volumes/retail_lakehouse/landing/products.csv"
)

# COMMAND ----------

order_facts = build_order_facts(silver_orders, products)

(
    order_facts.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date")
    .saveAsTable(f"{catalog}.gold.fact_orders")
)

# COMMAND ----------

daily_summary = daily_sales_summary(order_facts)

(
    daily_summary.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.daily_sales_summary")
)

display(spark.table(f"{catalog}.gold.daily_sales_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer lifetime value (bonus gold table)
# MAGIC A second, small gold table — total spend and order count per customer —
# MAGIC useful to show a second reporting angle beyond time-based aggregation.

# COMMAND ----------

from pyspark.sql import functions as F

customer_ltv = (
    order_facts.filter(~F.col("status").isin("CANCELLED", "RETURNED"))
    .groupBy("customer_id")
    .agg(
        F.sum("line_amount").alias("lifetime_value"),
        F.countDistinct("order_id").alias("total_orders"),
    )
    .orderBy(F.col("lifetime_value").desc())
)

customer_ltv.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.gold.customer_ltv")

display(customer_ltv)
