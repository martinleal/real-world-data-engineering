import logging
from openaq import OpenAQ # type: ignore
import os
from datetime import datetime


class OpenAQClient:
    def __init__(self, api_key: str):
        self.client = OpenAQ(api_key=api_key)

    def fetch_countries(self):
        try:
            logging.info("Fetching countries")
            return self.client.countries.list()
        except Exception as e:
            logging.error(f"Failed to fetch countries: {e}")
            return None

    def fetch_instruments(self):
        try:
            logging.info("Fetching instruments")
            return self.client.instruments.list()
        except Exception as e:
            logging.error(f"Failed to fetch instruments: {e}")
            return None

    def fetch_licenses(self):
        try:
            logging.info("Fetching licenses")
            return self.client.licenses.list()
        except Exception as e:
            logging.error(f"Failed to fetch licenses: {e}")
            return None

    def fetch_locations(self, page=1, limit=1000):
        try:
            logging.info("Fetching locations")
            return self.client.locations.list(page=page, limit=limit)
        except Exception as e:
            logging.error(f"Failed to fetch locations: {e}")
            return None

    def fetch_manufacturers(self):
        try:
            logging.info("Fetching manufacturers")
            return self.client.manufacturers.list()
        except Exception as e:
            logging.error(f"Failed to fetch manufacturers: {e}")
            return None

    def fetch_owners(self):
        try:
            logging.info("Fetching owners")
            return self.client.owners.list()
        except Exception as e:
            logging.error(f"Failed to fetch owners: {e}")
            return None

    def fetch_parameters(self):
        try:
            logging.info("Fetching parameters")
            return self.client.parameters.list()
        except Exception as e:
            logging.error(f"Failed to fetch parameters: {e}")
            return None

    def fetch_providers(self):
        try:
            logging.info("Fetching providers")
            return self.client.providers.list()
        except Exception as e:
            logging.error(f"Failed to fetch providers: {e}")
            return None

    def fetch_sensors_for_location(self, location_id: int):
        try:
            logging.info(f"Fetching sensors for location_id={location_id}")
            return self.client.locations.sensors(location_id)
        except Exception as e:
            logging.error(f"Failed to fetch sensors for location_id={location_id}: {e}")
            return None

    def fetch_latest_measurements(self, sensor_id: int, datetime_from="2016-10-10"):
        try:
            logging.info(f"Fetching latest measurements for sensor_id={sensor_id}")
            now = datetime.utcnow()
            delta = datetime.strptime(datetime_from, "%Y-%m-%d")

            time_diff = now - delta

            return self.client.measurements.list(
                sensors_id=sensor_id,
                data="measurements",
                datetime_from=now - time_diff,
                datetime_to=now,
                limit=1000
            )
        except Exception as e:
            logging.error(f"Failed to fetch measurements for sensor_id={sensor_id}: {e}")
            return None

    def close(self):
        self.client.close()
