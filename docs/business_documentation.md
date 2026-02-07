# Business Documentation

## Executive Summary
The pipeline delivers validated and analytics-ready tax data in a lakehouse layout, with curated dimensions, historical facts, and a datamart table for reporting. It supports time trends, demographic analysis, geographic contribution, and compliance monitoring.

## Business Objectives and Coverage
### 1) Tax collection trends over time
- Source: `fact_tax_returns` and `datamart_tax_returns`
- Metrics: total tax paid, tax payable, returns count
- Time axes: `assessment_year` and `filing_date`

### 2) Income and tax patterns across demographics
- Source: `dim_taxpayer` + `fact_tax_returns` (or datamart)
- Attributes: `full_name`, `filing_status`, `residential_status`, `occupation`
- Metrics: average income, chargeable income, tax payable/paid

### 3) Geographic contribution to revenue
- Source: `dim_geo` + `fact_tax_returns` (or datamart)
- Attributes: `region`, `postal_code`
- Metrics: tax paid and tax payable by geography

### 4) Occupation vs tax compliance
- Source: datamart + data quality metrics
- Metrics: tax paid vs tax payable (compliance ratio), rule pass rates by occupation

## Outputs and Usage
- Curated tables are stable, versioned, and ready for analytics.
- Datamart table simplifies reporting by pre-joining dimensions to facts.
- Data quality outputs provide transparency on rule compliance and error categories.

## Data Quality and Governance
- Validation rules cover completeness, validity, and accuracy.
- Quarantine outputs isolate records that fail rules.
- Summary reports provide run-level metrics and error breakdowns.

## Reporting in Power BI
- Recommended to expose curated outputs via Fabric SQL views (see `docs/fabric_powerbi_setup.md`).
- Power BI connects to the Lakehouse SQL endpoint and uses views as a semantic layer.

## Key Assumptions
- Input data conforms to the configured column mapping.
- `assessment_year` is the primary incremental watermark.
- The pipeline is the system of record for curated and datamart outputs.

## Risks and Considerations
- If upstream schemas change, update `configs/pipeline.yaml` mappings.
- High frequency refresh needs orchestration in Fabric or ADF.
- Historical tracking for `dim_taxpayer` uses load timestamp ordering.
