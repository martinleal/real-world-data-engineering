## Architecture Overview:

                +-------------------------+               +------------------------------+
                |     OpenAQ API         |               |  OpenAQ Public AWS S3 Bucket |
                |  (Near-Real-Time Data) |               |  (Historical JSON files)     |
                +-----------+------------+               +--------------+---------------+
                            |                                             |
                            v                                             v
             +-------------------------------+        +----------------------------------+
             | Airflow DAG (nrt_air_quality) |        | Airflow DAG (historical_ingest) |
             | - Calls API                   |        | - Weekly load of old data        |
             | - Writes JSON to S3           |        | - Used for DQ + backfills        |
             +-------------------------------+        +----------------------------------+
                            |                                             |
                            +----------------------+----------------------+
                                                   |
                                                   v
                                          +--------------------+
                                          |      AWS S3        |
                                          |  (Raw Landing Zone)|
                                          +---------+----------+
                                                    |
                                    (Snowpipe auto-detection or trigger)
                                                    v
                                      +--------------------------+
                                      |        Snowflake         |
                                      |--------------------------|
                                      |       Bronze Layer       |
                                      |            v             |
                                      |       Silver Layer       |
                                      |            v             |
                                      |        Gold Layer        |
                                      +--------------------------+
                                                    |
                                                    v
                                  +-----------------------------------+
                                  |  BI Layer (Power BI or Streamlit) |
                                  |  - Regional pollution dashboards  |
                                  |  - Health-related indicators      |
                                  +-----------------------------------+



## Project Milestones

### Phase 1: Setup & Core Infrastructure

**Goal: Set up tooling, environments, repos, and base structure to support dev.**

 - Define project scope in README. ✅

 - Set up project repo structure (src/, dags/, models/, utils/, etc.) ✅

 - Create Python virtual environment and requirements.txt

 - Create Airflow basic DAG structure

 - Set up AWS S3 bucket (with raw/, processed/ folders)

 - Create Snowflake schema structure for bronze/silver/gold layers

 - Set up Snowpipe for raw load from S3

### Phase 2: Near-Real-Time Pipeline (Days 6–12)

**Goal: Show working E2E ingestion from API to Snowflake via Snowpipe.**

 - Build Airflow DAG to call OpenAQ API and store data in S3 (JSON or Parquet)

 - Configure Snowpipe to detect new files and load raw table

 - Create bronze & silver transformations in SQL (initial cleaned data model)

 - Basic data quality checks in Airflow (e.g., row count, schema match)

 - Add minimal unit test coverage

### Phase 3: Historical Backfill Pipeline (Days 13–17)
**Goal: Add long-term reliability + allow DQ and completeness checks.**

 - Build Airflow DAG to pull from public OpenAQ S3 bucket

 - Transform & load historical data into same bronze layer

 - Compare NRT vs historical to validate ingestion completeness

 - Add logic to detect and fill gaps from historical files if needed

### Phase 4: Modeling & Analytics (Days 18–23)
**Goal: Build a clear star schema with useful indicators.**

- Model fact table for pollution events (region, time, pollutant, etc.)

- Build dimension tables: region/location, time, pollutant type

- Define KPI layer in gold: average pollution, alerts, trends

- Optionally: Add basic health-related indicator (e.g., daily alert flags)

### Phase 5: Visualization Layer (Days 24–28)
**Goal: Show that your data pipeline delivers real, visible value.**

 - Choose BI tool: Power BI (cleaner for external use) or Streamlit (flexible, easier deploy)

 - Build at least 1 or 2 simple dashboards:

    - Air quality trends by region

    - % days above pollution threshold

 - Add screenshots and README with BI examples

### Phase 6: Define next objectives

 - Improve code readability: Refactor Airflow DAGs and Python scripts to enhance maintainability and clarity

 - Explore additional data sources: Integrate new datasets such as health metrics to enable deeper and more insightful visualizations

 - Evaluate scalability tools: Assess the use of Docker and Kafka to support higher scalability and system robustness.