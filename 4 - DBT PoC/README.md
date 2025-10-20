# DBT Flights PoC

This is a **proof of concept (PoC)** project demonstrating **dbt** for building a data warehouse with Snowflake. It processes flight data into a star schema, using incremental loads, Slowly Changing Dimensions (SCD), and comprehensive data quality checks.

## Overview

The pipeline transforms raw flight data into analytics-ready tables:
- **Bronze Layer**: Raw data from Snowflake external stage.
- **Silver Layer**: Cleaned and standardized staging data.
- **Gold Layer**: Dimensional model (star schema) with facts and dimensions.

## Architecture

### Data Flow
1. **Source**: Flight data loaded into Snowflake via external stage (`bronze_layer.raw_flights`).
2. **Silver**: `silver_flights` model cleans and types data, handles incremental updates.
3. **Gold**:
   - Dimensions: `dim_airport` (SCD2), `dim_city`, `dim_country`, `dim_flight_delay_type`, `dim_currency`, `dim_weather`.
   - Fact: `fact_flights` (incremental, joins to dimensions).
4. **Snapshots**: `airport_snapshot` for SCD2 history.
5. **Tests**: Custom SQL tests for consistency, freshness, and schema validations.

## Setup
1. Clone the repo and navigate to the project folder.
2. Install dependencies: `pip install -r requirements.txt`
3. Configure dbt profile in `~/.dbt/profiles.yml` or via environment variables.
4. Run `dbt deps` to install packages (e.g., dbt_utils).
5. Load initial data into Snowflake external stage.

## How to Run
1. **Build the silver model**: `dbt run --select silver_flights` (model with cleaned data).
2. **Run snapshots**: `dbt snapshot` (for SCD updates).
3. **Build the gold layer**: `dbt run --select gold`(analytics-ready star schema model).
3. **Test data quality**: `dbt test` (runs all tests).
4. **Generate docs**: `dbt docs generate && dbt docs serve` (view lineage and descriptions).

## Folder Structure
```
├── data/                          # Static source data
├── data_to_load/                  # Generated incremental CSVs
├── models/
│   ├── silver/                    # Staging layer
│   │   ├── silver_flights.sql     # Incremental cleaning
│   │   └── schema.yml             # Docs and tests
│   └── gold/                      # Core layer
│       ├── dim_*.sql              # Dimension models
│       ├── fact_flights.sql       # Fact model
│       └── schema.yml             # Docs and tests
├── snapshots/                     # SCD snapshots
│   └── airport_snapshot.sql
├── tests/                         # Custom data tests
│   ├── flights_consistency.sql    # Source vs. fact check
│   ├── test_freshness.sql         # Data recency
│   └── test_dim_airport_scd2_no_overlaps.sql  # SCD validation
├── packages.yml                   # dbt packages (dbt_utils)
├── dbt_project.yml                # Project config
└── README.md
```
