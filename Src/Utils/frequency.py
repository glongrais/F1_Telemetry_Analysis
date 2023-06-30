import pandas as pd
import json

file = '/Users/guillaumelongrais/Documents/Code/Python/F1_Telemetry_Analysis/Resources/lap_times.json'
data = {}
with open(file, 'r') as f:
    data = json.load(f)
df = pd.DataFrame(data["10"]).T

df = df["Date"]

# Assuming the date values are stored in a list called "date_values"
date_values = [
    1685278984025,
    1685278984305,
    1685278984545,
    1685278984785,
    1685278985105
]

# Convert the date values to pandas datetime format
dates = pd.to_datetime(df, unit='ms')

# Convert the datetime index to a series
dates_series = pd.Series(dates)

# Calculate the time differences between consecutive measurements
time_diff = dates_series.diff()

# Determine the average time difference in millisecond
average_frequency_s = time_diff.mean().total_seconds()
average_frequency_ms = average_frequency_s * 1000

# Print the average frequency in milliseconds
print("Average Frequency:", average_frequency_ms, "ms")

print("Average FPS:", 1.0/average_frequency_s, "fps")