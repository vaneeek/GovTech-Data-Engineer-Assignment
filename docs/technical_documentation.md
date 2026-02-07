# Technical Documentation

## Purpose
This document describes the current implementation of the GovTech tax data pipeline, including processing stages, data model, storage layout, and operational behavior.

## Pipeline Stages
1. Ingest
   - Reads CSV from `input/individual_tax_returns.csv` or a folder of CSVs.
   - Normalizes column names to snake_case.
   - Applies column mappings from `configs/pipeline.yaml`.
   - Tracks source file IDs when incremental file tracking is enabled.
2. Validate
   - Applies validation rules (format, logical, and cross-field checks).
   - Produces `validated`, `staging`, and `quarantine` datasets.
   - Calculates data quality metrics by domain.
3. Transform
   - Builds dimensions and facts from validated records.
   - Adds audit fields (`created_run_id`, `last_seen_run_id`, `created_at`, `updated_at`).
4. Write
   - Writes landing, raw, staging, quarantine, curated, and datamart outputs.
   - Upserts curated dimensions and facts.
   - Manages SCD2 for fact table and `dim_taxpayer`.
   - Updates incremental state and processed file ledger.

## Storage Layout (Lakehouse Zones)
- Landing: `outputs/landing/Tax_source/YYYY/MM/DD/{run_id}/*.csv`
- Raw: `outputs/raw/Tax_source/ingest_date=YYYY-MM-DD/*.parquet`
- Staging: `outputs/staging/Tax_source/ingest_date=YYYY-MM-DD/*.parquet`
- Quarantine: `outputs/staging/quarantine/Tax_source/ingest_date=YYYY-MM-DD/*.parquet`
- Curated:
  - `outputs/curated/dim_geo.parquet`
  - `outputs/curated/dim_taxpayer.parquet`
  - `outputs/curated/fact_tax_returns.parquet`
  - `outputs/curated/data_quality_results.parquet`
  - `outputs/curated/agg_data_quality_metrics.parquet`
  - `outputs/curated/summary_report.parquet`
- Datamart: `outputs/datamart/datamart_tax_returns.parquet`

## Data Model
### dim_geo
- `geo_id` (stable hash of postal_code)
- `postal_code`
- `region`
- Audit: `created_run_id`, `last_seen_run_id`, `created_at`, `updated_at`
   - Note: district-to-region mapping is currently hard coded in code. This can be replaced with a reference table to allow updates when planning areas change.

### dim_taxpayer (SCD2)
- Natural key: `nric`
- Attributes: `full_name`, `filing_status`, `residential_status`, `number_of_dependents`, `occupation`, `postal_code`, `housing_type`, `geo_id`
- Audit: `created_run_id`, `last_seen_run_id`, `created_at`, `updated_at`
- SCD2: `version`, `effective_start`, `effective_end`, `is_current`
- Ordering: `updated_at` (load timestamp)

### fact_tax_returns (SCD2)
- Natural key: `return_key` (hash of `nric` + `assessment_year`)
- Attributes: `taxpayer_id`, `assessment_year`, `filing_date`, income and tax measures, `foreign_income`
- Audit: `created_run_id`, `last_seen_run_id`, `created_at`, `updated_at`
- SCD2: `version`, `effective_start`, `effective_end`, `is_current`
- Ordering: `filing_date` (fallback to `created_at`)

### Datamart
`datamart_tax_returns` is a join of fact + current `dim_taxpayer` + `dim_geo`.

## SCD2 Behavior
- New versions are created when attribute values change for a natural key.
- `effective_end` is the next version start; current rows use `2262-04-11` as a safe max timestamp.
- For replaced rows:
  - `last_seen_run_id` remains from the last run in which that version appeared.
  - `updated_at` is set when the row is retired.

## Incremental Processing
- Watermark on `assessment_year` with optional backfill.
- File tracking ledger stored in `outputs/metadata/processed_files.csv`.
- State stored in `outputs/metadata/state.json`.

## Timezone
All run timestamps are recorded in Asia/Singapore and stored as ISO-8601 strings or timezone-aware timestamps in Parquet.

## Configuration
Main config file: `configs/pipeline.yaml`
- Input path, output paths, and lakehouse layers
- Column mapping
- Incremental configuration
- Data quality thresholds

## Operational Notes
- The input file is moved to `archive/` after each successful run.
- To re-run with the same file, move it back to `input/`.
- `scripts/read_parquet.py` can export curated and datamart data to CSV for quick inspection.
