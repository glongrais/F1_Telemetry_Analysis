import pandas as pd
import re
from typing import Dict, List, Optional, Tuple

_LAP_RE = re.compile(r"^(?:(\d+):)?(\d+):(\d+\.?\d*)$")


def _parse_lap_time(val):
    # type: (str) -> Optional[float]
    """Parse a lap/sector time string like '1:28.877' or '28.531' to seconds."""
    if not val:
        return None
    m = _LAP_RE.match(val.strip())
    if not m:
        return None
    minutes = int(m.group(1)) if m.group(1) else 0
    seconds = int(m.group(2))
    frac = float(m.group(3)) - int(float(m.group(3)))  # handle '28.531'
    # Actually, group(3) is the seconds with decimals
    return minutes * 60 + float(m.group(2)) + frac


def _parse_time_str(val):
    # type: (str) -> Optional[float]
    """Parse time strings like '1:28.877' or '28.531' into total seconds."""
    if not val:
        return None
    val = val.strip()
    parts = val.split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        elif len(parts) == 1:
            return float(parts[0])
        else:
            return None
    except (ValueError, TypeError):
        return None


class _DriverState:
    """Tracks the current timing state for one driver."""

    def __init__(self, driver_number):
        # type: (int) -> None
        self.driver_number = driver_number
        self.driver_code = ""
        self.team = ""
        self.current_lap = 0
        self.position = None
        self.in_pit = False
        self.pit_out = False
        # Sector times for current lap
        self.sectors = [None, None, None]  # type: List[Optional[str]]
        self.speeds = {"I1": None, "I2": None, "FL": None, "ST": None}
        self.last_lap_time = None  # type: Optional[str]
        self.personal_fastest = False
        # Stint tracking
        self.stint = 0
        self.compound = None
        self.tyre_life = 0
        self.fresh_tyre = None
        # Grid position
        self.grid_position = None
        # Completed laps
        self.completed = []  # type: List[dict]

    def _emit_lap(self):
        """Emit the current lap as a completed row."""
        if self.current_lap < 1:
            return
        row = {
            "DriverNumber": self.driver_number,
            "LapNumber": self.current_lap,
            "Driver": self.driver_code or None,
            "Team": self.team or None,
            "LapTime": pd.Timedelta(seconds=_parse_time_str(self.last_lap_time)) if _parse_time_str(self.last_lap_time) else pd.NaT,
            "Sector1Time": pd.Timedelta(seconds=_parse_time_str(self.sectors[0])) if _parse_time_str(self.sectors[0]) else pd.NaT,
            "Sector2Time": pd.Timedelta(seconds=_parse_time_str(self.sectors[1])) if _parse_time_str(self.sectors[1]) else pd.NaT,
            "Sector3Time": pd.Timedelta(seconds=_parse_time_str(self.sectors[2])) if _parse_time_str(self.sectors[2]) else pd.NaT,
            "SpeedI1": self.speeds.get("I1"),
            "SpeedI2": self.speeds.get("I2"),
            "SpeedFL": self.speeds.get("FL"),
            "SpeedST": self.speeds.get("ST"),
            "Compound": self.compound,
            "TyreLife": self.tyre_life,
            "FreshTyre": self.fresh_tyre,
            "Stint": self.stint,
            "Position": self.position,
            "IsPersonalBest": self.personal_fastest,
        }
        self.completed.append(row)

    def update_timing_data(self, data):
        # type: (dict) -> None
        """Process an incremental TimingData update for this driver."""
        if "RacingNumber" in data:
            pass  # already known
        if "Position" in data:
            try:
                self.position = int(data["Position"])
            except (ValueError, TypeError):
                pass
        if "InPit" in data:
            self.in_pit = data["InPit"]
        if "PitOut" in data:
            self.pit_out = data["PitOut"]

        # Sectors: can be list or dict
        sectors_data = data.get("Sectors")
        if sectors_data:
            if isinstance(sectors_data, list):
                items = enumerate(sectors_data)
            else:
                items = ((int(k), v) for k, v in sectors_data.items())
            for idx, sec in items:
                if idx < 3 and isinstance(sec, dict):
                    val = sec.get("Value")
                    if val:
                        self.sectors[idx] = val

        # Speeds
        speeds_data = data.get("Speeds")
        if speeds_data and isinstance(speeds_data, dict):
            for key in ("I1", "I2", "FL", "ST"):
                sp = speeds_data.get(key)
                if isinstance(sp, dict) and sp.get("Value"):
                    self.speeds[key] = sp["Value"]

        # Lap time
        last_lap = data.get("LastLapTime")
        if isinstance(last_lap, dict):
            val = last_lap.get("Value")
            if val:
                self.personal_fastest = last_lap.get("PersonalFastest", False)
                # New lap time means previous lap is complete
                if val != self.last_lap_time:
                    self.last_lap_time = val
                    self._emit_lap()
                    # Reset for next lap
                    self.sectors = [None, None, None]
                    self.speeds = {"I1": None, "I2": None, "FL": None, "ST": None}
                    self.personal_fastest = False

        # Number of laps (sometimes present)
        n_laps = data.get("NumberOfLaps")
        if n_laps is not None:
            try:
                new_lap = int(n_laps)
                if new_lap > self.current_lap:
                    self.current_lap = new_lap
            except (ValueError, TypeError):
                pass

    def update_timing_app_data(self, data):
        # type: (dict) -> None
        """Process an incremental TimingAppData update for this driver."""
        if "GridPos" in data:
            try:
                self.grid_position = int(data["GridPos"])
            except (ValueError, TypeError):
                pass

        # Stints
        stints = data.get("Stints")
        if stints:
            # Can be list [] or dict {"0": {...}}
            if isinstance(stints, list):
                items = stints
            elif isinstance(stints, dict):
                items = list(stints.values())
            else:
                items = []
            if items:
                latest = items[-1] if isinstance(items[-1], dict) else {}
                if "Compound" in latest:
                    self.compound = latest["Compound"]
                if "New" in latest:
                    self.fresh_tyre = latest["New"] == "true" if isinstance(latest["New"], str) else bool(latest["New"])
                if "TotalLaps" in latest:
                    try:
                        self.tyre_life = int(latest["TotalLaps"])
                    except (ValueError, TypeError):
                        pass
                self.stint = len(items) if isinstance(stints, list) else max(int(k) for k in stints.keys()) + 1


class LapAccumulator:
    """Accumulates incremental TimingData and TimingAppData messages into complete laps."""

    def __init__(self):
        self._drivers = {}  # type: Dict[int, _DriverState]

    def _get_driver(self, drv_num):
        # type: (int) -> _DriverState
        if drv_num not in self._drivers:
            self._drivers[drv_num] = _DriverState(drv_num)
        return self._drivers[drv_num]

    def process_timing_data(self, messages):
        # type: (List[Tuple[float, dict]]) -> None
        """Process all TimingData messages."""
        for ts, data in messages:
            lines = data.get("Lines", {})
            for drv_str, drv_data in lines.items():
                try:
                    drv_num = int(drv_str)
                except (ValueError, TypeError):
                    continue
                state = self._get_driver(drv_num)
                if "Tla" in drv_data:
                    state.driver_code = drv_data["Tla"]
                state.update_timing_data(drv_data)

    def process_timing_app_data(self, messages):
        # type: (List[Tuple[float, dict]]) -> None
        """Process all TimingAppData messages."""
        for ts, data in messages:
            lines = data.get("Lines", {})
            for drv_str, drv_data in lines.items():
                try:
                    drv_num = int(drv_str)
                except (ValueError, TypeError):
                    continue
                state = self._get_driver(drv_num)
                state.update_timing_app_data(drv_data)

    def set_driver_info(self, driver_number, code, team):
        # type: (int, str, str) -> None
        state = self._get_driver(driver_number)
        state.driver_code = code
        state.team = team

    def finalize(self):
        # type: () -> pd.DataFrame
        """Return all completed laps as a DataFrame matching db_writer.insert_laps."""
        all_rows = []
        for state in self._drivers.values():
            all_rows.extend(state.completed)
        if not all_rows:
            return pd.DataFrame()
        return pd.DataFrame(all_rows)
