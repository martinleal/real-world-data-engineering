# real-world-data-engineering

This repository features practical, production-level data pipelines and transformations based on real-world scenarios.  

Each example reflects optimizations, design patterns, and engineering decisions used in modern data platforms, particularly in Snowflake.

The goal is to share reusable ideas, not just code, including performance improvements, modular design, and data warehousing best practices.

## Contents

### 1. Optimized Fact Table Pipeline
A performance-optimized version of a fact table pipeline implemented in Snowflake.

**Key highlights:**
- Use of append-only `STREAMs` for incremental change tracking
- Use of deltas to detect changes on views, avoiding `STREAM` limitations
- Early flattening of semi-structured data
- Breaking down transformations into logical steps using temporary tables

**Folder contains:**
- `fact_table_deprecated_version/` – legacy version with performance issues
- `optimized_fact_table/` – modular version with ~70% performance improvement

### 2. Air Quality Indicators (In Progress)
A data engineering project that integrates air pollution metrics to generate region-level indicators for environmental risk monitoring and analysis.

**Key highlights:**
- Combines batch and streaming pipelines using public and near-real-time data sources.
- Ingests near-real-time API data, writes to S3, and loads into Snowflake using Snowpipe.
- Loads historical data from a public AWS Open Data S3 bucket.
- Applies Medallion Architecture and Star Schema modeling for scalable, analytics-ready datasets.
- Implements data quality checks and orchestration through Airflow for reliability and observability.

### 3. IaC - Infrastructure as Code (In Progess)
A dedicated section for **cloud infrastructure configuration** to support the projects in this repository.  

**Structure and goals:**
- `iac/air-quality/` – AWS resources for the Air Quality Health Project (e.g., S3 buckets, IAM roles, API Gateway, Lambda, Snowflake integrations).
- `iac/shared/` – reusable modules for common resources like networking, logging, and security.
- Modular and environment-based design to follow best practices for scalability and maintainability.
- Will be built with **Terraform** to keep infra consistent, version-controlled, and portable.

This IaC layer will ensure any pipeline or project in the repo can be deployed reproducibly, making it easier to share, test, and extend.