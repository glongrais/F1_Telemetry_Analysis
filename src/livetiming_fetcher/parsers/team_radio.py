import pandas as pd
from typing import List, Tuple

BASE_URL = "https://livetiming.formula1.com/static"


def parse_team_radio(messages, t0, session_path):
    # type: (List[Tuple[float, dict]], float, str) -> pd.DataFrame
    """Parse TeamRadio messages into a DataFrame matching db_writer.insert_team_radio."""
    rows = []
    for ts, data in messages:
        captures = data.get("Captures", {})
        items = captures if isinstance(captures, list) else captures.values()
        for cap in items:
            path = cap.get("Path", "")
            rows.append({
                "RacingNumber": cap.get("RacingNumber"),
                "Path": f"{BASE_URL}/{session_path}{path}",
                "Time": pd.Timedelta(seconds=ts - t0),
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
