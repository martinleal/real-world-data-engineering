import os
import subprocess
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# You can change this command to run different dbt commands as needed
'''
dbt run --full-refresh
dbt run --select silver_flights --full-refresh
dbt run --select gold
dbt snapshot
dbt snapshot --h
dbt build
dbt compile --select fact_flights
dbt test --select fact_flights
dbt test --show-all-deprecations
dbt deps
'''
command_list = [
    #"dbt run --select silver_flights --full-refresh",
    #"dbt snapshot --select airport_snapshot",
    #"dbt run --select gold --full-refresh",
    # "dbt test --select flights_freshness"
    # "dbt compile --select flights_consistency"
    # "dbt deps"
    "dbt docs generate",
    "dbt docs serve"
]

# Change working directory to dbt project
os.chdir(Path(__file__).parent)


# Run commands
for command in command_list:
    print(f"⚡ Running: {command}")
    subprocess.run(command, shell=True, check=True)
