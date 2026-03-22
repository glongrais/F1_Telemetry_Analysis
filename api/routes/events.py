from fastapi import APIRouter, Query

from api.db import query

router = APIRouter()

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
