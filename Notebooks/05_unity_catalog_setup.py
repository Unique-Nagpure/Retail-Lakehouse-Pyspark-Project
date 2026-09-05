# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Unity Catalog Setup
# MAGIC
# MAGIC Sets up the three-level namespace (`catalog.schema.table`) and basic
# MAGIC access grants used by the rest of the pipeline. Run this notebook first,
# MAGIC before 01-04, on a fresh workspace.
# MAGIC
# MAGIC Demonstrates governance concepts commonly asked about in Databricks
# MAGIC interviews: catalog/schema creation, external volumes for landing data,
# MAGIC and role-based `GRANT` statements.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS retail_lakehouse;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_lakehouse.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_lakehouse.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### External volume for landing raw files
# MAGIC A managed Volume gives Auto Loader and the streaming job a governed
# MAGIC path to read from, instead of a raw cloud path with no Unity Catalog
# MAGIC lineage or access control.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retail_lakehouse.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS retail_lakehouse.landing.raw_files;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example role-based grants
# MAGIC In an interview, be ready to explain the principle: grant the narrowest
# MAGIC scope needed (e.g. read-only on gold for BI consumers, no access at all
# MAGIC to bronze for most roles).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data engineers: full read/write on all layers
# MAGIC GRANT USE CATALOG ON CATALOG retail_lakehouse TO `data-engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA retail_lakehouse.bronze TO `data-engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA retail_lakehouse.silver TO `data-engineers`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA retail_lakehouse.gold TO `data-engineers`;
# MAGIC
# MAGIC -- BI / analytics consumers: read-only on gold only
# MAGIC GRANT USE CATALOG ON CATALOG retail_lakehouse TO `bi-analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA retail_lakehouse.gold TO `bi-analysts`;
# MAGIC GRANT SELECT ON SCHEMA retail_lakehouse.gold TO `bi-analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Note:** `data-engineers` and `bi-analysts` are placeholder Unity
# MAGIC > Catalog group names — replace with real groups from your workspace's
# MAGIC > account console before running.
