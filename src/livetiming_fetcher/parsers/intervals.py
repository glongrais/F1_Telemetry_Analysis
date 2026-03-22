import pandas as pd
from typing import List, Tuple


def parse_intervals(messages, t0):
    # type: (List[Tuple[float, dict]], float) -> pd.DataFrame
    """Parse TimingData messages into an intervals DataFrame for Race/Sprint sessions.

    Extracts GapToLeader, IntervalToPositionAhead, Position per driver per timestamp.
    Matches db_writer.insert_intervals contract.
    """
    rows = []
    for ts, data in messages:
        lines = data.get("Lines", {})
        for drv_str, drv_data in lines.items():
            # Only emit rows that have gap/interval data
            gap = drv_data.get("GapToLeader")
            interval = drv_data.get("IntervalToPositionAhead")
            position = drv_data.get("Position")

            if gap is None and interval is None and position is None:
                continue

            interval_val = None
            if isinstance(interval, dict):
                interval_val = interval.get("Value")
            elif isinstance(interval, str):
                interval_val = interval

            try:
                drv_num = int(drv_str)
            except (ValueError, TypeError):
                continue

            try:
                pos = int(position) if position else None
            except (ValueError, TypeError):
                pos = None

            rows.append({
                "DriverNumber": drv_num,
                "Time": pd.Timedelta(seconds=ts - t0),
                "Position": pos,
                "GapToLeader": gap if isinstance(gap, str) else (str(gap) if gap is not None else None),
                "IntervalToPositionAhead": interval_val if interval_val else None,
            })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
