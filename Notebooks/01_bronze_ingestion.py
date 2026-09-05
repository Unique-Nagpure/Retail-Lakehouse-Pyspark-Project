# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Raw Ingestion with Auto Loader
# MAGIC
# MAGIC Ingests raw order events (JSON) landing in a cloud storage path using
# MAGIC **Auto Loader** (`cloudFiles`). Demonstrates:
# MAGIC - Incremental, checkpointed file discovery (no full-folder rescans)
# MAGIC - Schema inference + evolution handling
# MAGIC - Rescued-data column for fields not in the known schema (e.g. `promo_code`
# MAGIC   appearing only in later files)
# MAGIC
# MAGIC In this repo, `data/sample/orders_raw/` simulates a landing zone that
# MAGIC receives new files over time (batch_1, batch_2, ...).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retail_lakehouse")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("source_path", "/Volumes/retail_lakehouse/landing/orders_raw")
dbutils.widgets.text("checkpoint_path", "/Volumes/retail_lakehouse/checkpoints/bronze_orders")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

target_table = f"{catalog}.{schema}.orders_bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto Loader stream: raw JSON -> Bronze Delta table
# MAGIC Runs as a `.trigger(availableNow=True)` batch-style stream so it can be
# MAGIC scheduled as a Databricks Job without needing an always-on cluster.

# COMMAND ----------

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(source_path)
    .withColumn("_ingest_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

(
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick sanity check

# COMMAND ----------

display(spark.table(target_table).orderBy(F.col("order_ts").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** any column not present in the initial inferred schema (like
# MAGIC `promo_code`, only present in `orders_batch_2.json`) is captured under
# MAGIC `_rescued_data` rather than silently dropped or failing the pipeline —
# MAGIC this is the behavior worth calling out in an interview when asked about
# MAGIC schema drift handling.
