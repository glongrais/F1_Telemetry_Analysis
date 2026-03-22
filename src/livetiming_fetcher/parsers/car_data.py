import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Tuple

# Channel number to column name mapping
CHANNELS = {
    0: "RPM",
    2: "Speed",
    3: "nGear",
    4: "Throttle",
    5: "Brake",
    45: "DRS",
}


def parse_car_data(messages, t0):
    # type: (List[Tuple[float, dict]], float) -> Dict[int, pd.DataFrame]
    """Parse CarData.z messages into a dict of {driver_number: DataFrame}.

    Returns the same shape as session.car_data in FastF1 so db_writer.insert_car_data_bulk works.
    """
    # Collect rows per driver
    per_driver = {}  # type: Dict[int, list]

    for ts, data in messages:
        entries = data.get("Entries", [])
        for entry in entries:
            utc_str = entry.get("Utc", "")
            try:
                utc_dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                utc_dt = None

            cars = entry.get("Cars", {})
            for drv_str, car in cars.items():
                try:
                    drv = int(drv_str)
                except (ValueError, TypeError):
                    continue

                channels = car.get("Channels", {})
                # Channels keys can be int or str depending on JSON parsing
                ch = {}
                for k, v in channels.items():
                    ch[int(k)] = v

                row = {
                    "Time": pd.Timedelta(seconds=ts - t0),
                    "Date": utc_dt,
                    "Speed": ch.get(2),
                    "RPM": ch.get(0),
                    "nGear": ch.get(3),
                    "Throttle": ch.get(4),
                    "Brake": bool(ch.get(5, 0)),
                    "DRS": ch.get(45),
                    "Source": "livetiming",
                }

                if drv not in per_driver:
                    per_driver[drv] = []
                per_driver[drv].append(row)

    # Convert to DataFrames
    result = {}
    for drv, rows in per_driver.items():
        df = pd.DataFrame(rows)
        if not df.empty:
            result[drv] = df

    return result
