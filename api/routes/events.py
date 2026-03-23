import json
from datetime import datetime, date, timedelta

import requests
from fastapi import APIRouter, Query

from api.db import query

router = APIRouter()

_F1_SCHEDULE_URL = "https://livetiming.formula1.com/static/{year}/Index.json"

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


@router.get("/events")
def list_events(year: int = Query(default=2024)):
    rows = query(
        """
        SELECT
            e.round_number AS round,
            e.event_name AS name,
            e.country,
            e.location,
            e.event_date AS date,
            e.event_format AS format
        FROM events e
        WHERE e.year = ?
        ORDER BY e.round_number
        """,
        [year],
    )
    for row in rows:
        fmt = (row["format"] or "").lower()
        row["format"] = "sprint" if "sprint" in fmt else "conventional"
        if row["date"] is not None:
            row["date"] = str(row["date"])
        # Convert country name to ISO code for frontend flag rendering
        row["country"] = COUNTRY_CODES.get(row["country"], row["country"][:2].upper())
    return rows


@router.get("/events/{round_number}/sessions")
def list_sessions(round_number: int, year: int = Query(default=2024)):
    rows = query(
        """
        SELECT
            s.session_id AS "sessionId",
            s.session_type AS type,
            s.session_name AS name,
            s.date_start AS "dateStart",
            s.date_end AS "dateEnd"
        FROM sessions s
        JOIN events e ON s.event_id = e.event_id
        WHERE e.year = ? AND e.round_number = ?
        ORDER BY s.session_id
        """,
        [year, round_number],
    )
    for row in rows:
        if row["dateStart"] is not None:
            row["dateStart"] = str(row["dateStart"])
        if row["dateEnd"] is not None:
            row["dateEnd"] = str(row["dateEnd"])
    return rows


@router.get("/seasons")
def list_seasons():
    rows = query("SELECT DISTINCT year FROM events ORDER BY year DESC")
    return [row["year"] for row in rows]


@router.get("/next-race")
def next_race():
    """Return the next upcoming race with actual start time from F1 livetiming API,
    falling back to DB events if the API doesn't have it yet."""
    year = date.today().year
    today = date.today()

    # Try livetiming API first for exact start times
    try:
        r = requests.get(_F1_SCHEDULE_URL.format(year=year), timeout=10)
        r.raise_for_status()
        cal = json.loads(r.content.decode("utf-8-sig"))

        now = datetime.utcnow()
        for meeting in cal.get("Meetings", []):
            code = meeting.get("Code", "")
            name = meeting.get("Name", "")
            if "T" in code.replace("F1", "").replace(str(meeting.get("Number", "")), "") or "Testing" in name:
                continue

            sessions = meeting.get("Sessions", [])
            race_session = next((s for s in sessions if s.get("Name") == "Race"), None)
            if not race_session:
                continue

            start = race_session.get("StartDate", "")
            if not start:
                continue

            race_start = datetime.fromisoformat(start)
            if race_start < now:
                continue

            gmt_offset = race_session.get("GmtOffset", "00:00:00")
            country = meeting.get("Country", {}).get("Name", "")
            event_format = "conventional"
            sess_names = {s.get("Name", "") for s in sessions}
            if "Sprint" in sess_names or "Sprint Qualifying" in sess_names:
                event_format = "sprint"

            return {
                "round": meeting.get("Number", 0),
                "name": meeting.get("Name", ""),
                "country": COUNTRY_CODES.get(country, country[:2].upper()),
                "location": meeting.get("Location", ""),
                "date": start,
                "gmtOffset": gmt_offset,
                "format": event_format,
            }
    except Exception:
        pass

    # Fallback: use DB events (date only, no start time)
    rows = query(
        """
        SELECT e.round_number, e.event_name, e.country, e.location,
               e.event_date, e.event_format
        FROM events e
        WHERE e.year = ? AND e.event_date >= ?
        ORDER BY e.event_date
        LIMIT 1
        """,
        [year, str(today)],
    )
    if not rows:
        return None

    row = rows[0]
    fmt = (row["event_format"] or "").lower()
    country = row["country"] or ""
    return {
        "round": row["round_number"],
        "name": row["event_name"],
        "country": COUNTRY_CODES.get(country, country[:2].upper()),
        "location": row["location"],
        "date": str(row["event_date"]),
        "gmtOffset": None,
        "format": "sprint" if "sprint" in fmt else "conventional",
    }
