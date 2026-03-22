import pandas as pd
from typing import List, Tuple


def parse_session_status(messages, t0):
    # type: (List[Tuple[float, dict]], float) -> pd.DataFrame
    """Parse SessionStatus messages into a DataFrame matching db_writer.insert_session_status."""
    rows = []
    for ts, data in messages:
        status = data.get("Status")
        if status:
            rows.append({
                "Time": pd.Timedelta(seconds=ts - t0),
                "Status": status,
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def find_session_start(messages):
    # type: (List[Tuple[float, dict]]) -> float
    """Find the relative timestamp of SessionStatus 'Started'. Returns 0.0 if not found."""
    for ts, data in messages:
        if data.get("Status") == "Started":
            return ts
    return 0.0
