import os
from pathlib import Path
from dotenv import load_dotenv # type: ignore
from api_client import OpenAQClient
from datetime import timedelta
from utils import write_to_file, setup_logging


if __name__ == "__main__":
    setup_logging()

    load_dotenv()
    API_KEY = os.getenv("OPENAQ_API_KEY")
    ingestor = OpenAQClient(api_key=API_KEY)

    # call dimensions
    countries = ingestor.fetch_countries()
    instruments = ingestor.fetch_instruments()
    licenses = ingestor.fetch_licenses()
    locations = ingestor.fetch_locations()
    manufacturers = ingestor.fetch_manufacturers()
    owners = ingestor.fetch_owners()
    parameters = ingestor.fetch_parameters()
    providers = ingestor.fetch_providers()

    RAW_DATA_DIR = Path("data/raw")
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ## write dimensions
    write_to_file(countries, RAW_DATA_DIR, 'countries')
    write_to_file(instruments, RAW_DATA_DIR, 'instruments')
    write_to_file(licenses, RAW_DATA_DIR, 'licenses')
    write_to_file(locations, RAW_DATA_DIR, 'locations')
    write_to_file(manufacturers, RAW_DATA_DIR, 'manufacturers')
    write_to_file(owners, RAW_DATA_DIR, 'owners')
    write_to_file(parameters, RAW_DATA_DIR, 'parameters')
    write_to_file(providers, RAW_DATA_DIR, 'providers')
    
    # call sensors by location
    sensors = ingestor.fetch_sensors_for_location(location_id=3)
    write_to_file(sensors, RAW_DATA_DIR, 'sensors_by_location')

    # call measurements by sensor
    measurements = ingestor.fetch_latest_measurements(sensor_id=52)
    write_to_file(measurements, RAW_DATA_DIR, 'measurements_by_sensor')
