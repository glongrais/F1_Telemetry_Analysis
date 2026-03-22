import pandas as pd
from typing import List, Tuple


def parse_race_control(messages, t0):
    # type: (List[Tuple[float, dict]], float) -> pd.DataFrame
    """Parse RaceControlMessages into a DataFrame matching db_writer.insert_race_control."""
    rows = []
    for ts, data in messages:
        msgs = data.get("Messages", {})
        # Messages can be a list or dict keyed by index
        items = msgs if isinstance(msgs, list) else msgs.values()
        for msg in items:
            rows.append({
                "Time": pd.Timedelta(seconds=ts - t0),
                "Category": msg.get("Category", ""),
                "Message": msg.get("Message", ""),
                "Status": msg.get("Status"),
                "Flag": msg.get("Flag"),
                "Scope": msg.get("Scope"),
                "Sector": msg.get("Sector"),
                "RacingNumber": msg.get("RacingNumber"),
                "Lap": msg.get("Lap"),
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
