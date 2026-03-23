from datetime import date
from pathlib import Path
from typing import List, Optional

import duckdb

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "f1_data_v2.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


def query(sql: str, params: Optional[List] = None) -> List[dict]:
    conn = get_connection()
    try:
        result = conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        conn.close()


def current_year() -> int:
    return date.today().year


# --- Shared utilities used by multiple route modules ---

COUNTRY_CODES = {
    "Bahrain": "BH", "Saudi Arabia": "SA", "Australia": "AU", "Japan": "JP",
    "China": "CN", "United States": "US", "Italy": "IT", "Monaco": "MC",
    "Canada": "CA", "Spain": "ES", "Austria": "AT", "United Kingdom": "GB",
    "Hungary": "HU", "Belgium": "BE", "Netherlands": "NL", "Azerbaijan": "AZ",
    "Singapore": "SG", "Mexico": "MX", "Brazil": "BR", "Qatar": "QA",
    "Abu Dhabi": "AE", "United Arab Emirates": "AE", "Portugal": "PT",
    "France": "FR", "Russia": "RU", "Turkey": "TR", "Germany": "DE",
    "Emilia Romagna": "IT", "Miami": "US", "Las Vegas": "US",
}


def color_hex(raw) -> str:
    """Normalize a team_color value to '#RRGGBB' format.
    Handles values that may or may not already have a '#' prefix."""
    if raw is None:
        return "#000000"
    s = str(raw).lstrip("#")
    return f"#{s}"


def format_lap_time(seconds) -> str:
    if seconds is None:
        return ""
    minutes = int(seconds // 60)
    secs = seconds - minutes * 60
    if minutes > 0:
        return f"{minutes}:{secs:06.3f}"
    return f"{secs:.3f}"


def format_gap(seconds) -> str:
    if seconds is None:
        return ""
    if seconds == 0:
        return ""
    return f"+{seconds:.3f}"
