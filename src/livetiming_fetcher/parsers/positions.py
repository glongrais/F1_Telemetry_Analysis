import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple


def parse_positions(messages, t0):
    # type: (List[Tuple[float, dict]], float) -> Dict[int, pd.DataFrame]
    """Parse Position.z messages into a dict of {driver_number: DataFrame}.

    Wire format: {"Position": [{"Timestamp": "2026-...", "Entries": {"1": {"X": n, "Y": n, "Z": n, "Status": "..."}}}]}
    Returns the same shape as session.pos_data in FastF1 so db_writer.insert_locations_bulk works.
    """
    per_driver = {}  # type: Dict[int, list]

    for ts, data in messages:
        position_list = data.get("Position", [])
        for entry in position_list:
            utc_str = entry.get("Timestamp", "")
            try:
                utc_dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                utc_dt = None

            cars = entry.get("Entries", {})
            for drv_str, pos in cars.items():
                try:
                    drv = int(drv_str)
                except (ValueError, TypeError):
                    continue

                row = {
                    "Time": pd.Timedelta(seconds=ts - t0),
                    "Date": utc_dt,
                    "X": pos.get("X"),
                    "Y": pos.get("Y"),
                    "Z": pos.get("Z"),
                    "Status": pos.get("Status", ""),
                    "Source": "livetiming",
                }

                if drv not in per_driver:
                    per_driver[drv] = []
                per_driver[drv].append(row)

    result = {}
    for drv, rows in per_driver.items():
        df = pd.DataFrame(rows)
        if not df.empty:
            result[drv] = df

    return result
