# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Structured Streaming: Near-Real-Time Order Monitoring
# MAGIC
# MAGIC A separate streaming pipeline (distinct from the batch-style Auto Loader
# MAGIC job in notebook 01) that computes a **5-minute windowed order count and
# MAGIC revenue** with a watermark to handle late-arriving events — the pattern
# MAGIC an interviewer will typically ask about when probing structured streaming
# MAGIC knowledge.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

dbutils.widgets.text("catalog", "retail_lakehouse")
dbutils.widgets.text("source_path", "/Volumes/retail_lakehouse/landing/orders_raw")
dbutils.widgets.text("checkpoint_path", "/Volumes/retail_lakehouse/checkpoints/streaming_orders")

catalog = dbutils.widgets.get("catalog")
source_path = dbutils.widgets.get("source_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("order_ts", TimestampType()),
    StructField("status", StringType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read stream with an explicit schema
# MAGIC (Using a fixed schema here rather than Auto Loader, to show both
# MAGIC ingestion styles in the same project — `readStream` on a file source
# MAGIC works the same way against a Kafka/Event Hubs source in production,
# MAGIC only the `.format(...)` and options change.)

# COMMAND ----------

raw_stream = (
    spark.readStream.format("json")
    .schema(order_schema)
    .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Watermark + 5-minute tumbling window aggregation
# MAGIC The watermark tells Spark to tolerate events up to 10 minutes late
# MAGIC before finalizing a window's results, bounding state size.

# COMMAND ----------

windowed_counts = (
    raw_stream
    .filter(F.col("status") != "CANCELLED")
    .withWatermark("order_ts", "10 minutes")
    .groupBy(F.window("order_ts", "5 minutes"), "status")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("quantity").alias("total_units"),
    )
)

# COMMAND ----------

query = (
    windowed_counts.writeStream.format("delta")
    .outputMode("update")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="30 seconds")
    .toTable(f"{catalog}.gold.streaming_order_windows")
)

# COMMAND ----------

# MAGIC %md
# MAGIC For a demo/portfolio run, stop the stream after it has processed the
# MAGIC sample files rather than leaving it running indefinitely:
# MAGIC ```python
# MAGIC import time
# MAGIC time.sleep(60)
# MAGIC query.stop()
# MAGIC ```
