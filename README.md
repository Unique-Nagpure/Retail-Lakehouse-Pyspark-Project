# Retail Lakehouse Pipeline (PySpark + Databricks)

An end-to-end **medallion architecture** (bronze → silver → gold) data pipeline
built with PySpark on Databricks, using a small synthetic retail/e-commerce
dataset (customers, products, orders).

This project is a portfolio piece demonstrating hands-on PySpark and Databricks
skills: Auto Loader ingestion, Delta Lake merges (SCD Type 2), Structured
Streaming with watermarking, Unity Catalog governance, and job orchestration.

## Architecture

```
                 ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
  Raw JSON/CSV → │   BRONZE    │  →   │   SILVER    │  →   │    GOLD     │ → BI / reporting
  (landing zone) │ Auto Loader │      │ Clean+SCD2  │      │ Facts+Aggs  │
                 └─────────────┘      └─────────────┘      └─────────────┘

  Raw JSON     → ┌──────────────────────┐
  (order events)│  STRUCTURED STREAMING │ → windowed order counts (near-real-time)
                 └──────────────────────┘
```

- **Bronze**: Raw ingestion via Auto Loader (`cloudFiles`), with schema
  evolution / rescued-data handling for late-appearing fields.
- **Silver**: Data quality rules, deduplication of late/retried events, and an
  **SCD Type 2** merge for the customer dimension (full change history).
- **Gold**: A daily sales summary by category and a customer lifetime-value
  table — the layer a BI tool would query directly.
- **Streaming**: A separate Structured Streaming job computing 5-minute
  windowed order metrics with a watermark for late data.
- **Unity Catalog**: Catalog/schema/volume setup and example role-based grants.

## Repo layout

```
retail-lakehouse-pyspark/
├── notebooks/              # Databricks notebooks, run in numeric order
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_aggregates.py
│   ├── 04_streaming_orders.py
│   └── 05_unity_catalog_setup.py
├── src/
│   └── transformations.py  # Reusable, unit-tested PySpark functions
├── tests/
│   └── test_transformations.py
├── data/sample/            # Small synthetic sample data
│   ├── customers.csv
│   ├── customers_update.csv
│   ├── products.csv
│   └── orders_raw/          # Simulated incremental file drops for Auto Loader
├── jobs/
│   └── pipeline_job.json   # Databricks Jobs (workflow) definition
├── requirements.txt
└── README.md
```

## How to run on Databricks

1. **Import into a Repo**: Clone this repo into your Databricks workspace via
   Repos, or upload the `notebooks/` files directly.
2. **Upload sample data**: Copy the contents of `data/sample/` to a Unity
   Catalog Volume (e.g. `/Volumes/retail_lakehouse/landing/`), matching the
   paths referenced in the notebook widgets.
3. **Run in order**:
   - `05_unity_catalog_setup.py` (one-time setup)
   - `01_bronze_ingestion.py`
   - `02_silver_transform.py`
   - `03_gold_aggregates.py`
   - `04_streaming_orders.py` (independent streaming demo)
4. **Optional — orchestrate as a Job**: Import `jobs/pipeline_job.json` as a
   Databricks Job (Workflows → Create Job → Import from JSON), updating the
   notebook paths and cluster spec to match your workspace.

## How to run tests locally

The transformation logic in `src/transformations.py` is plain PySpark, so it
can be tested without a Databricks cluster:

```bash
pip install -r requirements.txt
pytest tests/
```

## Why this project

Built to demonstrate practical, Databricks-native PySpark skills relevant to
data engineering roles — Auto Loader, medallion architecture, Structured
Streaming, Delta Lake merge patterns (SCD Type 2), and Unity Catalog
governance — using a dataset and pipeline shape close to real-world retail
analytics use cases.
