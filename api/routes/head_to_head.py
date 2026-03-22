from fastapi import APIRouter, Query

from api.db import query

router = APIRouter()


@router.get("/head-to-head")
def head_to_head(year: int = Query(default=2024)):
    rows = query(
        """
        WITH latest_round AS (
            SELECT MAX(round_number) AS max_round
            FROM mart__head_to_head
            WHERE year = ?
        ),
        ytd AS (
            SELECT
                h.team_name,
                h.team_id,
                h.driver_1_code,
                h.driver_1_name,
                h.driver_2_code,
                h.driver_2_name,
                h.round_number,
                h.event_name,
                h.driver_1_quali_beat,
                h.driver_1_race_beat,
                '#' || MAX(d1.team_color) AS "teamColor"
            FROM mart__head_to_head h
            JOIN sessions s ON s.year = h.year
                AND s.round_number = h.round_number
                AND s.session_type = 'Race'
            JOIN drivers d1 ON d1.session_id = s.session_id
                AND d1.driver_number = h.driver_1_number
            WHERE h.year = ?
            GROUP BY h.team_name, h.team_id, h.driver_1_code, h.driver_1_name,
                     h.driver_2_code, h.driver_2_name, h.round_number,
                     h.event_name, h.driver_1_quali_beat, h.driver_1_race_beat
        )
        SELECT
            team_name,
            driver_1_code,
            driver_1_name,
            driver_2_code,
            driver_2_name,
            "teamColor",
            SUM(CASE WHEN driver_1_quali_beat THEN 1 ELSE 0 END) AS d1_quali_wins,
            SUM(CASE WHEN NOT driver_1_quali_beat THEN 1 ELSE 0 END) AS d2_quali_wins,
            SUM(CASE WHEN driver_1_race_beat THEN 1 ELSE 0 END) AS d1_race_wins,
            SUM(CASE WHEN NOT driver_1_race_beat THEN 1 ELSE 0 END) AS d2_race_wins
        FROM ytd
        GROUP BY team_name, driver_1_code, driver_1_name,
                 driver_2_code, driver_2_name, "teamColor"
        ORDER BY team_name
        """,
        [year, year],
    )
    result = []
    for row in rows:
        result.append(
            {
                "driver1": {
                    "abbreviation": row["driver_1_code"],
                    "teamColor": row["teamColor"],
                    "name": row["driver_1_name"],
                },
                "driver2": {
                    "abbreviation": row["driver_2_code"],
                    "teamColor": row["teamColor"],
                    "name": row["driver_2_name"],
                },
                "qualiScore": [row["d1_quali_wins"], row["d2_quali_wins"]],
                "raceScore": [row["d1_race_wins"], row["d2_race_wins"]],
                "rounds": [],  # Could populate per-round detail if needed
            }
        )
    return result
