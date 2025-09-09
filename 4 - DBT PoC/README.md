# dbt Flights Project

This project is a **proof of concept (PoC)** demonstrating how to use **dbt** with Snowflake for data modeling, testing, and documentation.  
The dataset simulates an airline scenario with incremental flight data arriving over time.  

---

## Pipeline Overview

1. **Source data**  
   - `flights.csv`: base dataset with flight details (downloaded from a public source).  

2. **Python preprocessing scripts** (simulate new data arriving):  
   - `add_timestamp_column.py`: adds a random `departure_timestamp` column to the base dataset.  
   - `generate_new_data.py`: generates a new CSV with a user-defined number of rows, randomly sampled from the base dataset, and assigns new random departure timestamps between a chosen start and end date. This simulates **incremental data arrivals**.  

   > All generated files are stored in the `data_to_load/` folder, which is configured as a **Snowflake external stage** for loading into the warehouse.

3. **Staging models**  
   - Clean and standardize raw fields from the CSVs.  

4. **Core models**  
   - `dim_airports`: unique list of airports.  
   - `dim_time`: derived calendar/time fields.  
   - `fact_flights`: fact table with metrics such as ticket price, delay minutes, and distance.  

5. **Tests**  
   - Uniqueness, non-null, and referential integrity checks to ensure data quality.  

6. **Documentation**  
   - `schema.yml` contains descriptions of datasets and columns for dbt Docs.  

---

## Key Features

- **Incremental ingestion** of flight data via generated CSVs.  
- **Star schema** modeling with dimensions and fact tables.  
- **Data quality tests** built into dbt.  
- **Documentation** using dbt’s native tools.  

---

## Folder Structure

├── data/ # Original source data (static CSVs)

├── data_to_load/ # Generated CSVs with timestamps (incremental data)

├── models/

│ ├── staging/ # Staging models

│ └── core/ # Dimensional and fact models

├── add_timestamp_column.py

├── generate_new_data.py

└── schema.yml