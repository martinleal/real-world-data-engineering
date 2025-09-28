import snowflake.connector
import os
import glob
from dotenv import load_dotenv
from pathlib import Path


load_dotenv()

warehouse="COMPUTE_WH"
database="DBT_POC"
schema="BRONZE_LAYER"
target_table = "RAW_FLIGHTS"

# Connect
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=warehouse,
    database=database,
    schema=schema
)
cur = conn.cursor()

cur.execute(f"USE DATABASE {database}")
cur.execute(f"USE SCHEMA {schema}")

# Create table (adjust schema to your CSV)
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        avgticketprice VARCHAR,
        cancelled VARCHAR,
        dest VARCHAR,
        destairportid VARCHAR,
        destcityname VARCHAR,
        destcountry VARCHAR,
        destlocation VARCHAR,
        destregion VARCHAR,
        destweather VARCHAR,
        distancekilometers VARCHAR,
        distancemiles VARCHAR,
        flightdelay VARCHAR,
        flightdelaymin VARCHAR,
        flightdelaytype VARCHAR,
        flightnum VARCHAR,
        flighttimehour VARCHAR,
        flighttimemin VARCHAR,
        origin VARCHAR,
        originairportid VARCHAR,
        origincityname VARCHAR,
        origincountry VARCHAR,
        originlocation VARCHAR,
        originregion VARCHAR,
        originweather VARCHAR,
        dayofweek VARCHAR,
        hour_of_day VARCHAR,
        departure_timestamp VARCHAR
    )
""")

# Get full CSV file path URI
base_dir = os.path.dirname(os.path.abspath(__file__))

data_dir = os.path.join(base_dir, "data_to_load")
csv_files = glob.glob(os.path.join(data_dir, "*.csv"))

print(f"📂 Found {len(csv_files)} CSV files to upload.")

for file_path in csv_files:
    file_path_uri = Path(file_path).as_posix()
    print(f"➡ Uploading {file_path_uri} ...")
    cur.execute(f"PUT 'file://{file_path_uri}' @%{target_table}")

# Load file into table
cur.execute(f"""
    copy into {target_table}
    from @%{target_table}
    file_format = (
        type = csv
        field_delimiter = ','
        field_optionally_enclosed_by = '"'
        skip_header = 1
        empty_field_as_null = true
        null_if = ('NULL', 'null')
    )
    force = false
""")

print("✅ Data loaded successfully!")

cur.close()
conn.close()
