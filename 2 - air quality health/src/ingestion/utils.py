import logging
import json
from datetime import datetime
from pathlib import Path

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def write_to_file(data, folder: Path, prefix: str):
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filename = f"{prefix}_{timestamp}.json"
    path = folder / filename

    # Ensure 'data' is a JSON-serializable dict
    if hasattr(data, "dict"):
        data = data.dict()

    with open(path, "w") as f:
        json.dump(data, f)
    
    logging.info(f"Wrote data to: {path}")
    return str(path)
