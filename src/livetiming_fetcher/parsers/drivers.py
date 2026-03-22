import pandas as pd
from typing import Dict, List, Tuple


def parse_drivers(messages, timing_messages=None):
    # type: (List[Tuple[float, dict]], List[Tuple[float, dict]]) -> pd.DataFrame
    """Parse DriverList messages into a DataFrame matching db_writer.insert_drivers.

    The first DriverList message has the full roster. Subsequent messages are
    incremental updates (e.g. position changes) which we merge.
    """
    drivers = {}  # type: Dict[str, dict]

    # Build driver roster from DriverList messages
    for ts, data in messages:
        for drv_str, info in data.items():
            if not isinstance(info, dict):
                continue
            if drv_str not in drivers:
                drivers[drv_str] = {}
            drivers[drv_str].update(info)

    # Extract final positions from TimingData if available
    final_positions = {}  # type: Dict[str, dict]
    if timing_messages:
        for ts, data in timing_messages:
            lines = data.get("Lines", {})
            for drv_str, drv_data in lines.items():
                if drv_str not in final_positions:
                    final_positions[drv_str] = {}
                if "Position" in drv_data:
                    final_positions[drv_str]["Position"] = drv_data["Position"]
                if "Retired" in drv_data:
                    final_positions[drv_str]["Retired"] = drv_data["Retired"]
                if "NumberOfLaps" in drv_data:
                    final_positions[drv_str]["NumberOfLaps"] = drv_data["NumberOfLaps"]

    rows = []
    for drv_str, info in drivers.items():
        racing_number = info.get("RacingNumber", drv_str)
        try:
            drv_num = int(racing_number)
        except (ValueError, TypeError):
            continue

        colour = info.get("TeamColour", "")
        if colour and not colour.startswith("#"):
            colour = "#" + colour

        # Position and laps from timing data
        pos_data = final_positions.get(drv_str, {})
        position = pos_data.get("Position")
        try:
            position = int(position) if position else None
        except (ValueError, TypeError):
            position = None

        n_laps = pos_data.get("NumberOfLaps")
        try:
            n_laps = int(n_laps) if n_laps else None
        except (ValueError, TypeError):
            n_laps = None

        retired = pos_data.get("Retired", False)
        status = "Retired" if retired else "Finished"

        row = {
            "DriverNumber": drv_num,
            "Abbreviation": info.get("Tla", ""),
            "BroadcastName": info.get("BroadcastName", ""),
            "FullName": info.get("FullName", ""),
            "FirstName": info.get("FirstName", ""),
            "LastName": info.get("LastName", ""),
            "TeamName": info.get("TeamName", ""),
            "TeamColor": colour,
            "HeadshotUrl": info.get("HeadshotUrl", ""),
            "Position": position,
            "Laps": n_laps,
            "Status": status,
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
