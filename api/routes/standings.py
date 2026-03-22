from fastapi import APIRouter, Query

from api.db import query

router = APIRouter()


@router.get("/standings/drivers")
def driver_standings(year: int = Query(default=2024)):
    rows = query(
        """
        WITH latest_round AS (
            SELECT MAX(round_number) AS max_round
            FROM mart__driver_standings
            WHERE year = ?
        )
        SELECT
            ds.championship_position AS position,
            ds.driver_code AS abbreviation,
            d.first_name AS "firstName",
            d.last_name AS "lastName",
            ds.team_name AS team,
            '#' || d.team_color AS "teamColor",
            ds.cumulative_points AS points,
            ds.cumulative_wins AS wins,
            d.country_code AS "countryCode"
        FROM mart__driver_standings ds
        JOIN latest_round lr ON ds.round_number = lr.max_round
        JOIN drivers d ON ds.session_id = d.session_id
            AND ds.driver_number = d.driver_number
        WHERE ds.year = ?
        ORDER BY ds.championship_position
        """,
        [year, year],
    )
    return rows


@router.get("/standings/constructors")
def constructor_standings(year: int = Query(default=2024)):
    rows = query(
        """
        WITH latest_round AS (
            SELECT MAX(round_number) AS max_round
            FROM mart__constructor_standings
            WHERE year = ?
        )
        SELECT
            cs.constructor_championship_position AS position,
            cs.team_name AS name,
            '#' || MAX(d.team_color) AS color,
            cs.team_cumulative_points AS points,
            cs.team_cumulative_wins AS wins
        FROM mart__constructor_standings cs
        JOIN latest_round lr ON cs.round_number = lr.max_round
        JOIN sessions s ON s.year = cs.year
            AND s.round_number = cs.round_number
            AND s.session_type = 'Race'
        JOIN drivers d ON d.session_id = s.session_id
            AND d.team_name = cs.team_name
        WHERE cs.year = ?
        GROUP BY cs.constructor_championship_position, cs.team_name,
                 cs.team_cumulative_points, cs.team_cumulative_wins
        ORDER BY cs.constructor_championship_position
        """,
        [year, year],
    )
    return rows


@router.get("/standings/drivers/evolution")
def driver_standings_evolution(year: int = Query(default=2024)):
    rows = query(
        """
        SELECT
            ds.round_number AS round,
            ds.event_name AS "raceName",
            ds.driver_code AS driver,
            ds.cumulative_points AS points
        FROM mart__driver_standings ds
        WHERE ds.year = ?
        ORDER BY ds.round_number, ds.championship_position
        """,
        [year],
    )
    # Pivot into {round, raceName, VER: points, NOR: points, ...}
    pivoted = {}
    colors = {}
    for row in rows:
        key = row["round"]
        if key not in pivoted:
            pivoted[key] = {"round": row["round"], "raceName": row["raceName"]}
        pivoted[key][row["driver"]] = row["points"]

    # Get driver colors
    color_rows = query(
        """
        SELECT DISTINCT ds.driver_code, '#' || d.team_color AS color
        FROM mart__driver_standings ds
        JOIN sessions s ON s.year = ds.year
            AND s.round_number = ds.round_number
            AND s.session_type = 'Race'
        JOIN drivers d ON d.session_id = s.session_id
            AND d.driver_number = ds.driver_number
        WHERE ds.year = ?
        """,
        [year],
    )
    for row in color_rows:
        colors[row["driver_code"]] = row["color"]

    return {"data": list(pivoted.values()), "colors": colors}


@router.get("/standings/constructors/evolution")
def constructor_standings_evolution(year: int = Query(default=2024)):
    rows = query(
        """
        SELECT
            cs.round_number AS round,
            cs.event_name AS "raceName",
            cs.team_name AS team,
            cs.team_cumulative_points AS points
        FROM mart__constructor_standings cs
        WHERE cs.year = ?
        ORDER BY cs.round_number, cs.constructor_championship_position
        """,
        [year],
    )
    pivoted = {}
    colors = {}
    for row in rows:
        key = row["round"]
        if key not in pivoted:
            pivoted[key] = {"round": row["round"], "raceName": row["raceName"]}
        pivoted[key][row["team"]] = row["points"]

    # Get constructor colors
    color_rows = query(
        """
        SELECT DISTINCT cs.team_name, '#' || MAX(d.team_color) AS color
        FROM mart__constructor_standings cs
        JOIN sessions s ON s.year = cs.year
            AND s.round_number = cs.round_number
            AND s.session_type = 'Race'
        JOIN drivers d ON d.session_id = s.session_id
            AND d.team_name = cs.team_name
        WHERE cs.year = ?
        GROUP BY cs.team_name
        """,
        [year],
    )
    for row in color_rows:
        colors[row["team_name"]] = row["color"]

    return {"data": list(pivoted.values()), "colors": colors}
