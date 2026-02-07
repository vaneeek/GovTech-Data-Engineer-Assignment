# Microsoft Fabric + Power BI Integration

## Purpose
This document describes how to expose curated and datamart outputs from the pipeline in Microsoft Fabric and make them consumable in Power BI using SQL views.

## Target Architecture
- Storage: Microsoft Fabric OneLake (Lakehouse)
- Serving: Lakehouse SQL endpoint (or Warehouse)
- Consumption: Power BI semantic model on SQL views

## Lakehouse Layout
Store outputs in OneLake under a Lakehouse named `tax-lakehouse` (example). Use the following folder layout:

- `Files/curated/dim_taxpayer/`
- `Files/curated/dim_geo/`
- `Files/curated/fact_tax_returns/`
- `Files/datamart/datamart_tax_returns/`

These folders should contain Parquet (or Delta) output files produced by the pipeline.

## Register Tables (Recommended)
In the Lakehouse UI, register each folder as a table for simpler SQL:

- `curated_dim_taxpayer`
- `curated_dim_geo`
- `curated_fact_tax_returns`
- `datamart_tax_returns`

## SQL Views (Lakehouse SQL Endpoint)
Create views that expose stable, Power BI friendly entities.

### If Tables Are Registered
```sql
CREATE OR ALTER VIEW dbo.dim_taxpayer_current AS
SELECT *
FROM curated_dim_taxpayer
WHERE is_current = 1;

CREATE OR ALTER VIEW dbo.dim_geo AS
SELECT *
FROM curated_dim_geo;

CREATE OR ALTER VIEW dbo.fact_tax_returns AS
SELECT *
FROM curated_fact_tax_returns;

CREATE OR ALTER VIEW dbo.datamart_tax_returns AS
SELECT *
FROM datamart_tax_returns;
```

### If Reading Parquet Directly
Replace `<workspace>` and `<lakehouse>` with your Fabric workspace and Lakehouse names.

```sql
CREATE OR ALTER VIEW dbo.dim_taxpayer_current AS
SELECT *
FROM OPENROWSET(
  BULK 'https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>/Files/curated/dim_taxpayer/*.parquet',
  FORMAT = 'PARQUET'
) AS t
WHERE is_current = 1;

CREATE OR ALTER VIEW dbo.dim_geo AS
SELECT *
FROM OPENROWSET(
  BULK 'https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>/Files/curated/dim_geo/*.parquet',
  FORMAT = 'PARQUET'
) AS t;

CREATE OR ALTER VIEW dbo.fact_tax_returns AS
SELECT *
FROM OPENROWSET(
  BULK 'https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>/Files/curated/fact_tax_returns/*.parquet',
  FORMAT = 'PARQUET'
) AS t;

CREATE OR ALTER VIEW dbo.datamart_tax_returns AS
SELECT *
FROM OPENROWSET(
  BULK 'https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>/Files/datamart/datamart_tax_returns/*.parquet',
  FORMAT = 'PARQUET'
) AS t;
```

## Power BI Connection
1. In Power BI Desktop: Get Data -> SQL Server.
2. Server: use the Lakehouse SQL endpoint (copy from the Fabric UI).
3. Database: select the Lakehouse database.
4. Select the views created above and load them into the model.

## Refresh Behavior
- On-demand: rerun the pipeline to write new files, and views will reflect the latest data.
- Scheduled: use Fabric pipelines or Power BI scheduled refresh if a fixed cadence is required.

## Recommended Views for Business Questions
- `dbo.datamart_tax_returns`: primary analytics table for most reporting needs.
- `dbo.dim_taxpayer_current`: current taxpayer attributes.
- `dbo.dim_geo`: regional enrichment.
- `dbo.fact_tax_returns`: detailed return history with SCD2 fields.

## Notes
- Ensure the Lakehouse SQL endpoint has read access to the OneLake paths.
- If you later move to a Warehouse, re-point the views to the Warehouse tables.
