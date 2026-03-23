from fastapi import APIRouter, Query

from api.db import query, format_lap_time, format_gap

router = APIRouter()


@router.get("/session/{session_id}/leaderboard")
def session_leaderboard(session_id: int):
    """Return leaderboard for qualifying or race session."""
    session_info = query(
        "SELECT session_type FROM sessions WHERE session_id = ?", [session_id]
    )
    if not session_info:
        return []

    session_type = session_info[0]["session_type"]

    if session_type in ("Qualifying", "Sprint Qualifying", "Sprint Shootout"):
        return _qualifying_leaderboard(session_id)
    else:
        return _race_leaderboard(session_id)


def _qualifying_leaderboard(session_id: int) -> list:
    rows = query(
        """
        SELECT
            q.qualifying_position AS position,
            q.full_name AS driver,
            q.driver_code AS abbreviation,
            q.team_name AS team,
            '#' || LTRIM(d.team_color, '#') AS "teamColor",
            q.q1,
            q.q2,
            q.q3,
            q.best_qualifying_time AS "bestLap",
            q.gap_to_pole AS "gapToLeader",
            q.furthest_session
        FROM int__qualifying_results q
        JOIN drivers d ON q.session_id = d.session_id
            AND q.driver_number = d.driver_number
        WHERE q.session_id = ?
        ORDER BY q.qualifying_position
        """,
        [session_id],
    )
    result = []
    for row in rows:
        entry = {
            "position": row["position"],
            "driver": row["driver"],
            "abbreviation": row["abbreviation"],
            "team": row["team"],
            "teamColor": row["teamColor"],
            "bestLap": format_lap_time(row["bestLap"]),
            "gapToLeader": format_gap(row["gapToLeader"]),
            "sector1": {"time": format_lap_time(row["q1"]), "status": "normal"},
            "sector2": {"time": format_lap_time(row["q2"]), "status": "normal"},
            "sector3": {"time": format_lap_time(row["q3"]), "status": "normal"},
            "laps": 0,
        }
        result.append(entry)
    return result


def _race_leaderboard(session_id: int) -> list:
    rows = query(
        """
        SELECT
            r.finish_position AS position,
            r.full_name AS driver,
            r.driver_code AS abbreviation,
            r.team_name AS team,
            '#' || LTRIM(d.team_color, '#') AS "teamColor",
            r.status,
            r.laps_completed AS laps,
            r.points,
            r.positions_gained AS "positionsGained"
        FROM int__race_results r
        JOIN drivers d ON r.session_id = d.session_id
            AND r.driver_number = d.driver_number
        WHERE r.session_id = ?
        ORDER BY r.finish_position
        """,
        [session_id],
    )
    result = []
    for row in rows:
        entry = {
            "position": row["position"],
            "driver": row["driver"],
            "abbreviation": row["abbreviation"],
            "team": row["team"],
            "teamColor": row["teamColor"],
            "bestLap": "",
            "gapToLeader": "",
            "status": row["status"],
            "laps": row["laps"],
            "points": row["points"],
            "positionsGained": row["positionsGained"],
            "sector1": {"time": "", "status": "normal"},
            "sector2": {"time": "", "status": "normal"},
            "sector3": {"time": "", "status": "normal"},
        }
        result.append(entry)
    return result


@router.get("/session/{session_id}/laps")
def session_laps(session_id: int):
    rows = query(
        """
        SELECT
            l.lap_number AS lap,
            l.driver_code AS driver,
            l.lap_time AS time
        FROM laps l
        WHERE l.session_id = ?
            AND l.lap_time IS NOT NULL
            AND l.is_accurate = TRUE
            AND l.deleted = FALSE
        ORDER BY l.lap_number, l.driver_code
        """,
        [session_id],
    )
    pivoted = {}
    for row in rows:
        key = row["lap"]
        if key not in pivoted:
            pivoted[key] = {"lap": key}
        pivoted[key][row["driver"]] = row["time"]
    return list(pivoted.values())


@router.get("/session/{session_id}/positions")
def session_positions(session_id: int):
    rows = query(
        """
        SELECT
            l.lap_number AS lap,
            l.driver_code AS driver,
            l.position
        FROM laps l
        WHERE l.session_id = ?
            AND l.position IS NOT NULL
        ORDER BY l.lap_number, l.position
        """,
        [session_id],
    )
    pivoted = {}
    for row in rows:
        key = row["lap"]
        if key not in pivoted:
            pivoted[key] = {"lap": key}
        pivoted[key][row["driver"]] = row["position"]
    return list(pivoted.values())


@router.get("/session/{session_id}/gaps")
def session_gaps(session_id: int):
    """Gap to leader per lap (sampled: one point per driver per lap)."""
    rows = query(
        """
        WITH lap_timestamps AS (
            SELECT
                session_id,
                lap_number,
                MAX(sector_3_session_time) AS lap_end_time
            FROM laps
            WHERE session_id = ?
            GROUP BY session_id, lap_number
        ),
        gap_per_lap AS (
            SELECT
                lt.lap_number AS lap,
                d.driver_code AS driver,
                g.gap_to_leader_seconds AS gap
            FROM int__gap_analysis g
            JOIN lap_timestamps lt ON g.session_id = lt.session_id
            JOIN drivers d ON g.session_id = d.session_id
                AND g.driver_number = d.driver_number
            WHERE g.session_id = ?
                AND g.gap_to_leader_seconds IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY lt.lap_number, g.driver_number
                ORDER BY ABS(g.timestamp - lt.lap_end_time)
            ) = 1
        )
        SELECT lap, driver, gap
        FROM gap_per_lap
        ORDER BY lap, gap
        """,
        [session_id, session_id],
    )
    pivoted = {}
    for row in rows:
        key = row["lap"]
        if key not in pivoted:
            pivoted[key] = {"lap": key}
        pivoted[key][row["driver"]] = row["gap"]
    return list(pivoted.values())


@router.get("/session/{session_id}/weather")
def session_weather(session_id: int):
    rows = query(
        """
        WITH numbered AS (
            SELECT
                w.timestamp,
                w.air_temperature AS "airTemp",
                w.track_temperature AS "trackTemp",
                w.humidity,
                w.pressure,
                w.wind_speed AS "windSpeed",
                w.wind_direction AS "windDirection",
                w.rainfall,
                ROW_NUMBER() OVER (ORDER BY w.timestamp) AS rn,
                COUNT(*) OVER () AS total
            FROM weather w
            WHERE w.session_id = ?
        )
        SELECT
            timestamp,
            "airTemp",
            "trackTemp",
            humidity,
            pressure,
            "windSpeed",
            "windDirection",
            rainfall
        FROM numbered
        WHERE rn % GREATEST(total / 50, 1) = 0 OR rn = 1
        ORDER BY timestamp
        """,
        [session_id],
    )
    for i, row in enumerate(rows):
        row["lap"] = i + 1
        row["timestamp"] = str(row["timestamp"])
    return rows


@router.get("/session/{session_id}/stints")
def session_stints(session_id: int):
    rows = query(
        """
        SELECT
            d.driver_code AS abbreviation,
            '#' || LTRIM(d.team_color, '#') AS "teamColor",
            d.full_name AS driver,
            st.stint,
            st.compound,
            st.lap_start AS "startLap",
            st.lap_end AS "endLap",
            st.lap_count AS laps
        FROM int__stints st
        JOIN drivers d ON st.session_id = d.session_id
            AND st.driver_number = d.driver_number
        WHERE st.session_id = ?
        ORDER BY d.position, st.stint
        """,
        [session_id],
    )
    drivers = {}
    for row in rows:
        abbr = row["abbreviation"]
        if abbr not in drivers:
            drivers[abbr] = {
                "driver": row["driver"],
                "abbreviation": abbr,
                "teamColor": row["teamColor"],
                "stints": [],
                "totalLaps": 0,
            }
        drivers[abbr]["stints"].append(
            {
                "compound": row["compound"],
                "startLap": row["startLap"],
                "endLap": row["endLap"],
                "laps": row["laps"],
            }
        )
        drivers[abbr]["totalLaps"] += row["laps"]
    return list(drivers.values())


@router.get("/session/{session_id}/pit-stops")
def session_pit_stops(session_id: int):
    rows = query(
        """
        WITH pit_ins AS (
            SELECT session_id, driver_number, lap_number AS lap, pit_in_time
            FROM int__pit_stops
            WHERE session_id = ? AND pit_in_time IS NOT NULL
        ),
        pit_outs AS (
            SELECT session_id, driver_number, lap_number AS lap, pit_out_time
            FROM int__pit_stops
            WHERE session_id = ? AND pit_out_time IS NOT NULL
        ),
        pit_data AS (
            SELECT
                pi.session_id,
                pi.driver_number,
                pi.lap,
                (po.pit_out_time - pi.pit_in_time) AS duration
            FROM pit_ins pi
            JOIN pit_outs po ON pi.session_id = po.session_id
                AND pi.driver_number = po.driver_number
                AND po.lap = pi.lap + 1
        )
        SELECT
            pd.lap,
            d.driver_code AS abbreviation,
            d.full_name AS driver,
            '#' || LTRIM(d.team_color, '#') AS "teamColor",
            pd.duration,
            before_stint.compound AS "tyreFrom",
            after_stint.compound AS "tyreTo"
        FROM pit_data pd
        JOIN drivers d ON pd.session_id = d.session_id
            AND pd.driver_number = d.driver_number
        LEFT JOIN int__stints before_stint ON pd.session_id = before_stint.session_id
            AND pd.driver_number = before_stint.driver_number
            AND pd.lap BETWEEN before_stint.lap_start AND before_stint.lap_end
        LEFT JOIN int__stints after_stint ON pd.session_id = after_stint.session_id
            AND pd.driver_number = after_stint.driver_number
            AND pd.lap + 1 BETWEEN after_stint.lap_start AND after_stint.lap_end
        ORDER BY pd.lap, d.driver_code
        """,
        [session_id, session_id],
    )
    for row in rows:
        if row["duration"] is not None:
            row["duration"] = round(row["duration"], 3)
    return rows


@router.get("/session/{session_id}/race-control")
def session_race_control(session_id: int):
    rows = query(
        """
        SELECT
            rc.race_control_id AS id,
            rc.timestamp,
            rc.lap_number AS lap,
            rc.incident_type AS category,
            rc.message,
            d.driver_code AS driver
        FROM int__race_control_incidents rc
        LEFT JOIN drivers d ON rc.session_id = d.session_id
            AND rc.driver_number = d.driver_number
        WHERE rc.session_id = ?
        ORDER BY rc.timestamp
        """,
        [session_id],
    )
    category_map = {
        "SafetyCar": "SAFETY_CAR",
        "VSC": "VSC",
        "RED_FLAG": "RED_FLAG",
        "DRS": "DRS_ENABLED",
        "PENALTY": "PENALTY",
        "INVESTIGATION": "PENALTY",
        "TRACK_LIMITS": "TRACK_LIMITS",
        "WARNING": "FLAG",
        "FLAGS": "FLAG",
        "OTHER": "INFO",
    }
    for row in rows:
        row["timestamp"] = str(row["timestamp"])
        row["category"] = category_map.get(row["category"], "INFO")
    return rows


@router.get("/session/{session_id}/radio")
def session_radio(session_id: int):
    rows = query(
        """
        SELECT
            tr.team_radio_id AS id,
            tr.timestamp,
            d.driver_code AS abbreviation,
            d.full_name AS driver,
            d.team_name AS team,
            '#' || LTRIM(d.team_color, '#') AS "teamColor",
            trt.transcription AS message,
            tr.recording_url AS "audioUrl"
        FROM team_radio tr
        JOIN drivers d ON tr.session_id = d.session_id
            AND tr.driver_number = d.driver_number
        LEFT JOIN team_radio_files trf ON tr.team_radio_id = trf.team_radio_id
        LEFT JOIN team_radio_texts trt ON trf.team_radio_file_id = trt.team_radio_file_id
        WHERE tr.session_id = ?
        ORDER BY tr.timestamp
        """,
        [session_id],
    )
    for row in rows:
        row["timestamp"] = str(row["timestamp"])
        row["lap"] = 0
    return rows


@router.get("/session/{session_id}/speed-traps")
def session_speed_traps(session_id: int):
    rows = query(
        """
        SELECT
            l.driver_code AS abbreviation,
            '#' || LTRIM(d.team_color, '#') AS "teamColor",
            MAX(l.speed_i1) AS "speedTrap1",
            MAX(l.speed_i2) AS "speedTrap2",
            MAX(l.speed_fl) AS "speedTrap3",
            MAX(l.speed_st) AS "speedTrap4",
            GREATEST(
                COALESCE(MAX(l.speed_i1), 0),
                COALESCE(MAX(l.speed_i2), 0),
                COALESCE(MAX(l.speed_fl), 0),
                COALESCE(MAX(l.speed_st), 0)
            ) AS "topSpeed"
        FROM laps l
        JOIN drivers d ON l.session_id = d.session_id
            AND l.driver_number = d.driver_number
        WHERE l.session_id = ?
        GROUP BY l.driver_code, d.team_color
        ORDER BY "topSpeed" DESC
        """,
        [session_id],
    )
    return rows


@router.get("/session/{session_id}/fastest-laps")
def session_fastest_laps(session_id: int):
    rows = query(
        """
        WITH ranked AS (
            SELECT
                l.driver_code AS abbreviation,
                d.team_name AS team,
                '#' || LTRIM(d.team_color, '#') AS "teamColor",
                l.lap_number AS "lapNumber",
                l.lap_time AS "lapTime",
                l.sector_1_time AS sector1,
                l.sector_2_time AS sector2,
                l.sector_3_time AS sector3,
                ROW_NUMBER() OVER (
                    PARTITION BY l.driver_code
                    ORDER BY l.lap_time
                ) AS driver_rank,
                RANK() OVER (ORDER BY l.lap_time) AS overall_rank
            FROM laps l
            JOIN drivers d ON l.session_id = d.session_id
                AND l.driver_number = d.driver_number
            WHERE l.session_id = ?
                AND l.lap_time IS NOT NULL
                AND l.is_accurate = TRUE
                AND l.deleted = FALSE
        )
        SELECT
            overall_rank AS position,
            abbreviation,
            team,
            "teamColor",
            "lapNumber",
            "lapTime",
            sector1,
            sector2,
            sector3
        FROM ranked
        WHERE driver_rank = 1
        ORDER BY overall_rank
        """,
        [session_id],
    )
    fastest = rows[0]["lapTime"] if rows else None
    for row in rows:
        gap = row["lapTime"] - fastest if fastest and row["lapTime"] else None
        row["gap"] = f"+{gap:.3f}" if gap and gap > 0 else ""
        row["lapTime"] = format_lap_time(row["lapTime"])
        row["sector1"] = format_lap_time(row["sector1"])
        row["sector2"] = format_lap_time(row["sector2"])
        row["sector3"] = format_lap_time(row["sector3"])
    return rows
