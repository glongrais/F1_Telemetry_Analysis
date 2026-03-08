WITH
laps AS (SELECT * FROM {{ ref('stg__laps') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }}),
events AS (SELECT * FROM {{ ref('stg__events') }}),
stints AS (SELECT * FROM {{ ref('int__stints') }}),
race_results AS (SELECT * FROM {{ ref('int__race_results') }}),
weather_summary AS (SELECT * FROM {{ ref('int__weather_summary') }}),
incidents AS (SELECT * FROM {{ ref('int__race_control_incidents') }}),

race_sessions AS (
    SELECT s.session_id, s.year, s.round_number, e.event_name, e.country, e.location
    FROM sessions s
    INNER JOIN events e ON s.event_id = e.event_id
    WHERE s.session_type = 'Race'
),

lap_stats AS (
    SELECT
        l.session_id,
        AVG(l.lap_time) AS avg_lap_time,
        MIN(l.lap_time) AS fastest_lap_time,
        AVG(l.speed_st) AS avg_speed_trap,
        MAX(l.speed_st) AS max_speed_trap,
        MIN(l.speed_st) AS min_speed_trap
    FROM laps l
    INNER JOIN race_sessions rs ON l.session_id = rs.session_id
    WHERE l.is_accurate = TRUE AND l.deleted = FALSE AND l.lap_time IS NOT NULL
    GROUP BY l.session_id
),

pit_stats AS (
    SELECT
        session_id,
        AVG(stint_count) AS avg_pit_stops,
        MODE(stint_count) AS most_common_pit_stops
    FROM (
        SELECT session_id, driver_number, MAX(stint) AS stint_count
        FROM stints
        GROUP BY session_id, driver_number
    ) sub
    GROUP BY session_id
),

position_stats AS (
    SELECT
        session_id,
        SUM(ABS(positions_gained)) AS total_position_changes,
        AVG(ABS(positions_gained)) AS avg_position_changes
    FROM race_results
    WHERE positions_gained IS NOT NULL
    GROUP BY session_id
),

incident_stats AS (
    SELECT
        session_id,
        COUNT(CASE WHEN incident_type = 'SAFETY_CAR' THEN 1 END) AS safety_car_count,
        COUNT(CASE WHEN incident_type = 'VIRTUAL_SAFETY_CAR' THEN 1 END) AS vsc_count,
        COUNT(CASE WHEN incident_type = 'RED_FLAG' THEN 1 END) AS red_flag_count
    FROM incidents
    GROUP BY session_id
)

SELECT
    rs.session_id,
    rs.year,
    rs.round_number,
    rs.event_name,
    rs.country,
    rs.location,
    ls.avg_lap_time,
    ls.fastest_lap_time,
    ls.avg_speed_trap,
    ls.max_speed_trap,
    ls.min_speed_trap,
    ps.avg_pit_stops,
    ps.most_common_pit_stops,
    pos.total_position_changes,
    pos.avg_position_changes,
    inc.safety_car_count,
    inc.vsc_count,
    inc.red_flag_count,
    ws.avg_air_temp,
    ws.avg_track_temp,
    ws.had_rainfall,
    ws.session_weather_class
FROM race_sessions rs
LEFT JOIN lap_stats ls ON rs.session_id = ls.session_id
LEFT JOIN pit_stats ps ON rs.session_id = ps.session_id
LEFT JOIN position_stats pos ON rs.session_id = pos.session_id
LEFT JOIN incident_stats inc ON rs.session_id = inc.session_id
LEFT JOIN weather_summary ws ON rs.session_id = ws.session_id
