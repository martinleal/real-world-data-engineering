import os
import subprocess
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Define dbt command sequence
command = "dbt run"
'''
dbt run --full-refresh
dbt run
dbt snapshot
dbt snapshot --h
dbt build
'''
# Change working directory to dbt project
os.chdir(Path(__file__).parent)


# Run commands
print(f"⚡ Running: {command}")
subprocess.run(command, shell=True, check=True)
