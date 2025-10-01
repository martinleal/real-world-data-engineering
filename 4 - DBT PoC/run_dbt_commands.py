import os
import subprocess
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# You can change this command to run different dbt commands as needed
'''
dbt run --full-refresh
dbt run
dbt snapshot
dbt snapshot --h
dbt build
'''
command_list = [
    "dbt run --select silver_flights",
    "dbt snapshot --select airport_snapshot",
    "dbt run --select gold+"
]

# Change working directory to dbt project
os.chdir(Path(__file__).parent)


# Run commands
for command in command_list:
    print(f"⚡ Running: {command}")
    subprocess.run(command, shell=True, check=True)
