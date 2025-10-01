import pandas as pd
import numpy as np
from datetime import datetime

def generate_increment(
    start_date: str,
    end_date: str,
    n_rows: int,
    source_csv: str = "source/flights.csv",
    output_dir: str = "data_to_load",
    seed: int = 42
) -> str:
    """
    Generate an incremental CSV with random rows and new random departure timestamps.

    Args:
        start_date: start datetime string (e.g. "2025-01-01 00:00:00").
        end_date: end datetime string (e.g. "2025-01-02 23:59:59").
        n_rows: number of rows to generate.
        source_csv: path to the source CSV with base flight data.
        output_dir: folder to save the new CSV.
        seed: random seed for reproducibility.

    Returns:
        Path to the generated CSV file.
    """
    # Load source
    df_source = pd.read_csv(source_csv)

    # Sample n rows (with replacement)
    rng = np.random.default_rng(seed)
    df_target = df_source.sample(n=n_rows, replace=True, random_state=seed).reset_index(drop=True)

    # Create random timestamps
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    total_seconds = int((end_ts - start_ts).total_seconds())
    offsets = rng.integers(0, total_seconds, size=n_rows)
    df_target["departure_timestamp"] = start_ts + pd.to_timedelta(offsets, unit="s")

    # Safe filename
    end_str = pd.to_datetime(end_date).strftime("%Y%m%d_%H%M%S")
    output_csv = f"{output_dir}/flights_{end_str}.csv"

    # Save
    df_target.to_csv(output_csv, index=False)
    print(f"✅ Generated {n_rows} rows from {start_date} to {end_date} -> {output_csv}")
    return output_csv


# Example usage:
if __name__ == "__main__":
    generate_increment(
        start_date="2025-01-02 00:00:00",
        end_date="2025-01-02 23:59:59",
        n_rows=120,
        source_csv="source/flights.csv",
        output_dir="data_to_load",
        seed=2
    )
