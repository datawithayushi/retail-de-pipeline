# retail-de-pipeline
Retail ELT pipeline on Databricks + GCS — bronze-to-gold Delta Lake tables, orchestrated via Databricks Workflows.

# ELT Data Pipeline for Daily Product Revenue

A batch data pipeline built on **Databricks (Unity Catalog) + Google Cloud Storage**, processing retail order data into daily product revenue metrics — orchestrated as a scheduled Databricks Workflow.

---

## Architecture (current)

```
Raw CSV (landing zone, GCS)
        │
        ▼
   ┌─────────┐
   │  BRONZE │  ◄─── orders, order_items, customers (raw, as-is)
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │  GOLD   │  ◄─── daily_product_revenue
   └─────────┘
```

## Tech Stack

| Layer | Tool |
|---|---|
| Compute | Databricks (Unity Catalog enabled) |
| Storage | Google Cloud Storage (Volumes) |
| Table format | Delta Lake |
| Transformation | PySpark / Spark SQL |
| Orchestration | Databricks Workflows (multi-task job) |
| Source data | Public retail dataset (orders, order_items, customers) |

## Pipeline Stages

**Bronze**: raw `orders`, `order_items`, and `customers` data converted from CSV to Parquet and loaded as managed Delta tables — no transformations, full fidelity to source.

**Gold**: `daily_product_revenue` — daily revenue aggregated by product, filtered to `COMPLETE`/`CLOSED` orders only.

```sql
CREATE OR REPLACE TABLE retail_de_project.gold.daily_product_revenue
USING DELTA
AS
SELECT o.order_date,
    oi.order_item_product_id,
    round(sum(oi.order_item_subtotal), 2) AS revenue
FROM retail_de_project.bronze.orders AS o
JOIN retail_de_project.bronze.order_items AS oi
    ON o.order_id = oi.order_item_order_id
WHERE o.order_status IN ('COMPLETE', 'CLOSED')
GROUP BY 1, 2
```

**Orchestration**: runs as a Databricks Workflow (job) with dependent tasks, job clusters, and retries configured.

## Key Design Decisions

**Managed Delta tables over external tables.** This pipeline was originally scaffolded using `CREATE EXTERNAL TABLE ... OPTIONS(path=...)` and `INSERT OVERWRITE DIRECTORY`, pointing at raw file paths. I migrated both to `CREATE TABLE ... USING DELTA` / `CREATE OR REPLACE TABLE ... AS SELECT`, because:
- Raw path references broke under Unity Catalog's DBFS-root restrictions
- Managed Delta tables get ACID transactions, schema enforcement, and time travel for free
- The gold table becomes queryable by name directly, no path lookups needed

## In Progress
- **Silver layer** — cleaning, deduplication, and type-casting between bronze and gold
- **SCD Type 2** on the `customers` dimension to track historical changes
- **Data quality checks** — null/referential integrity validation as a pipeline stage
- CI (GitHub Actions) to run unit tests on transformation logic

## Errors I Hit & Fixed
| Error | Root Cause | Fix |
|---|---|---|
| `PARSE_SYNTAX_ERROR` on `parquet.'path'` | Used single quotes instead of backticks | Backtick-quote the path |
| `UNABLE_TO_INFER_SCHEMA` | Pointed at a parent folder, not the folder with `.parquet` files | Pointed directly at the leaf folder |
| `InputWidgetNotDefined` | Referenced a widget before creating it | Created widgets before first use |
| `DBFS_DISABLED` | Empty widget collapsed path to bare DBFS root | Moved off `INSERT OVERWRITE DIRECTORY` to managed Delta tables |
| `Missing cloud file system scheme` | Referenced Unity Catalog's internal storage path directly | Learned managed vs external table storage boundaries |