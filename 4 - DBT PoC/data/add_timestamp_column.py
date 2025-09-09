import pandas as pd
import numpy as np


INPUT_CSV = "source/flights.csv"
# Column name and random range
COLUMN_NAME = "departure_timestamp"
START = "2024-01-01 00:00:00"
END = "2024-12-31 23:59:59"

end_str = pd.to_datetime(END).strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = f"data_to_load/flights_{end_str}.csv"



# Load file
df = pd.read_csv(INPUT_CSV)

# Convert start and end to timestamps
n = len(df)
rng = np.random.default_rng(100)
start_ts = pd.to_datetime(START)
end_ts = pd.to_datetime(END)

total_seconds = int((end_ts - start_ts).total_seconds())
offsets = rng.integers(0, total_seconds, size=n)
df["departure_timestamp"] = start_ts + pd.to_timedelta(offsets, unit="s")

# Save result
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Wrote {len(df)} rows with random timestamps to {OUTPUT_CSV}")
