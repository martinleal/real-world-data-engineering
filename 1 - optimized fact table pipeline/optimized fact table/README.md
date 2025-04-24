# Optimized Fact Table Pipeline

This folder contains a refactored version of a fact table pipeline implemented in Snowflake, redesigned for performance, flexibility, and improved data quality.

## 🔧 Background

The legacy pipeline had several limitations:
- **No use of `STREAM`s**: It filtered source data using a fixed time window (`CURRENT_TIMESTAMP - 2 hours`), leading to unreliable and inflexible processing.
- **Heavy workloads**: Complex computations led to remote spilling and high memory usage, impacting overall warehouse performance.
- **Basic data checks**: The only validation ensured that deleted records didn't appear in the target table — missing full coverage for insertions or late-arriving data.

## ✅ Key Improvements

- **Incremental processing with `STREAM`s**: Replaced time-window logic with Snowflake `STREAM`s for accurate, efficient change tracking.
- **Delta tracking system**: Detects changes in derived dimension data to ensure the fact table reflects the latest state.
- **Structured transformations**: Broke logic into modular steps with temporary tables to reduce memory usage and improve debugging.
- **Robust data validation**: Ensures every row from the source is accounted for in the destination, with logic to handle ingestion delays.
- **Environment-aware deployment**: Dynamic table naming allows code to run in both pre-production and production without changes.
- **Separation of concerns**: The final view is decoupled from internal logic and serves as the consumption layer prior to applying Row-Level Security (RLS).

## 🧱 Process Overview

- **Table & procedure setup**: Creates temporary tables and defines reusable procedures.
- **Data extraction**: A view pulls semi-structured source data.
- **Initial transformation**: Flattens JSON payloads and stores metadata in a `VARIANT` column.
- **Delta management**: Tracks changes in reference data and updates dimension tables.
- **Fact table update**: Applies inserts and updates to the fact table based on transformation results.
- **Consumption view**: Exposes a clean view of the fact table for downstream use.
- **Task orchestration**: Tasks execute the process in incremental or full-load mode (fulls only used after failures or stream resets).
