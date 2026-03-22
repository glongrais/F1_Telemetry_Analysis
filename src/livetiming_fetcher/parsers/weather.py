import pandas as pd
from typing import List, Tuple


def parse_weather(messages, t0):
    # type: (List[Tuple[float, dict]], float) -> pd.DataFrame
    """Parse WeatherData messages into a DataFrame matching db_writer.insert_weather."""
    rows = []
    for ts, data in messages:
        rows.append({
            "Time": pd.Timedelta(seconds=ts - t0),
            "AirTemp": float(data.get("AirTemp", 0)),
            "Humidity": float(data.get("Humidity", 0)),
            "Pressure": float(data.get("Pressure", 0)),
            "Rainfall": data.get("Rainfall", "0") == "1",
            "TrackTemp": float(data.get("TrackTemp", 0)),
            "WindDirection": int(float(data.get("WindDirection", 0))),
            "WindSpeed": float(data.get("WindSpeed", 0)),
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
