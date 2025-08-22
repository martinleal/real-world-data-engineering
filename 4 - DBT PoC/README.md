# dbt Flights Project

The goal of this project is to demonstrate a basic proof of concept (PoC) using **DBT Core for data modeling, testing, and documentation**. In the near future, I plan to develop more complex, production-level techniques as part of my Air Quality Indicators project.

## Pipeline overview
1. **Seed data** (`flights.csv`) with flight details
2. **Staging model**: clean and standardize raw fields
3. **Core models**:
   - `dim_airports`: unique list of airports
   - `dim_time`: derived calendar/time fields
   - `fact_flights`: fact table with metrics such as ticket price, delay minutes, and distance
4. **Tests**: uniqueness and consistency constraints
5. **Documentation**: schema.yml with descriptions

## How to run
```bash
dbt seed
dbt run
dbt test
