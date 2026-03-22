from fastapi import APIRouter, Query

from api.db import query
from api.routes.events import COUNTRY_CODES

router = APIRouter()


@router.get("/results/recent")
def recent_results(year: int = Query(default=2024), limit: int = Query(default=4)):
    rows = query(
        """
        WITH race_winners AS (
            SELECT
                r.round_number,
                r.event_name,
                r.country,
                r.driver_code,
                r.full_name,
                r.team_name,
                r.finish_position,
                r.finish_time
            FROM int__race_results r
            WHERE r.year = ? AND r.finish_position = 1
        ),
        fastest_laps AS (
            SELECT
                s.round_number,
                l.driver_code,
                l.lap_time,
                ROW_NUMBER() OVER (
                    PARTITION BY s.round_number
                    ORDER BY l.lap_time
                ) AS rn
            FROM laps l
            JOIN sessions s ON l.session_id = s.session_id
            WHERE s.year = ? AND s.session_type = 'Race'
                AND l.lap_time IS NOT NULL
                AND l.is_accurate = TRUE
                AND l.deleted = FALSE
        ),
        second_place AS (
            SELECT
                r.round_number,
                r.finish_time
            FROM int__race_results r
            WHERE r.year = ? AND r.finish_position = 2
        )
        SELECT
            rw.round_number AS round,
            rw.event_name AS name,
            rw.driver_code AS winner,
            rw.team_name AS "winnerTeam",
            rw.country AS "countryCode",
            fl.lap_time AS "fastestLap"
        FROM race_winners rw
        LEFT JOIN fastest_laps fl ON rw.round_number = fl.round_number AND fl.rn = 1
        ORDER BY rw.round_number DESC
        LIMIT ?
        """,
        [year, year, year, limit],
    )
    for row in rows:
        # Format fastest lap as string
        if row["fastestLap"] is not None:
            total_seconds = row["fastestLap"]
            minutes = int(total_seconds // 60)
            seconds = total_seconds - minutes * 60
            row["fastestLap"] = f"{minutes}:{seconds:06.3f}"
        row["gap"] = "+0.000s"
        # Convert country name to ISO code
        if row["countryCode"]:
            row["countryCode"] = COUNTRY_CODES.get(row["countryCode"], row["countryCode"][:2].upper())
    return rows
