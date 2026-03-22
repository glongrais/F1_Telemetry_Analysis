import pandas as pd
from typing import List, Tuple


def parse_track_status(messages, t0):
    # type: (List[Tuple[float, dict]], float) -> pd.DataFrame
    """Parse TrackStatus messages into a DataFrame matching db_writer.insert_track_status."""
    rows = []
    for ts, data in messages:
        rows.append({
            "Time": pd.Timedelta(seconds=ts - t0),
            "Status": data.get("Status", ""),
            "Message": data.get("Message", ""),
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
