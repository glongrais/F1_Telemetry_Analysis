from fastapi import APIRouter, Query

from api.db import query

router = APIRouter()


@router.get("/session/{session_id}/telemetry")
def session_telemetry(
    session_id: int,
    drivers: str = Query(description="Comma-separated driver codes, e.g. VER,NOR"),
    lap: int = Query(description="Lap number"),
):
    driver_list = [d.strip() for d in drivers.split(",")]
    placeholders = ", ".join(["?"] * len(driver_list))

    rows = query(
        f"""
        WITH driver_numbers AS (
            SELECT driver_number, driver_code, '#' || LTRIM(team_color, '#') AS team_color
            FROM drivers
            WHERE session_id = ?
                AND driver_code IN ({placeholders})
        ),
        lap_bounds AS (
            SELECT
                l.driver_number,
                l.lap_start_date AS start_ts,
                LEAD(l.lap_start_date) OVER (
                    PARTITION BY l.driver_number ORDER BY l.lap_number
                ) AS end_ts
            FROM laps l
            WHERE l.session_id = ?
                AND l.lap_number = ?
        )
        SELECT
            dn.driver_code AS abbreviation,
            dn.team_color AS "teamColor",
            cd.speed,
            cd.throttle,
            CASE WHEN cd.brake THEN 100 ELSE 0 END AS brake,
            cd.rpm,
            cd.n_gear AS gear,
            cd.drs
        FROM car_data cd
        JOIN driver_numbers dn ON cd.driver_number = dn.driver_number
        JOIN lap_bounds lb ON cd.driver_number = lb.driver_number
        WHERE cd.session_id = ?
            AND cd.date >= lb.start_ts
            AND (lb.end_ts IS NULL OR cd.date < lb.end_ts)
        ORDER BY dn.driver_code, cd.date
        """,
        [session_id] + driver_list + [session_id, lap, session_id],
    )

    # Group by driver and add distance approximation
    drivers_data = {}
    for row in rows:
        abbr = row["abbreviation"]
        if abbr not in drivers_data:
            drivers_data[abbr] = {
                "abbreviation": abbr,
                "teamColor": row["teamColor"],
                "data": [],
            }
        drivers_data[abbr]["data"].append(
            {
                "speed": row["speed"],
                "throttle": row["throttle"],
                "brake": row["brake"],
                "rpm": row["rpm"],
                "gear": row["gear"],
                "drs": row["drs"],
            }
        )

    # Add distance as evenly spaced values (approximate)
    for driver in drivers_data.values():
        n = len(driver["data"])
        if n > 0:
            # Approximate lap distance based on sample count
            # Most F1 circuits are 4-7km, use 5.4km as default
            lap_distance = 5.4
            for i, point in enumerate(driver["data"]):
                point["distance"] = round(i * lap_distance / n, 4)

    return list(drivers_data.values())


@router.get("/session/{session_id}/available-laps")
def available_laps(session_id: int, driver: str = Query()):
    rows = query(
        """
        SELECT DISTINCT l.lap_number
        FROM laps l
        JOIN drivers d ON l.session_id = d.session_id
            AND l.driver_number = d.driver_number
        WHERE l.session_id = ?
            AND d.driver_code = ?
            AND l.lap_time IS NOT NULL
        ORDER BY l.lap_number
        """,
        [session_id, driver],
    )
    return [row["lap_number"] for row in rows]
