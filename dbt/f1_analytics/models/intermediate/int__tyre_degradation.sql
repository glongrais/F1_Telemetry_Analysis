WITH
laps AS (SELECT * FROM {{ ref('stg__laps') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }})

SELECT
    l.session_id,
    s.year,
    s.round_number,
    s.session_type,
    l.driver_number,
    l.driver_code,
    l.lap_number,
    l.stint,
    l.compound,
    l.tyre_life,
    l.lap_time,
    ROW_NUMBER() OVER (
        PARTITION BY l.session_id, l.driver_number, l.stint
        ORDER BY l.lap_number
    ) AS stint_lap_number,
    LAG(l.lap_time) OVER (
        PARTITION BY l.session_id, l.driver_number, l.stint
        ORDER BY l.lap_number
    ) AS prev_lap_time,
    l.lap_time - LAG(l.lap_time) OVER (
        PARTITION BY l.session_id, l.driver_number, l.stint
        ORDER BY l.lap_number
    ) AS lap_time_delta,
    l.lap_time - FIRST_VALUE(l.lap_time) OVER (
        PARTITION BY l.session_id, l.driver_number, l.stint
        ORDER BY l.lap_number
    ) AS delta_from_stint_start
FROM laps l
INNER JOIN sessions s ON l.session_id = s.session_id
WHERE
    l.is_accurate = TRUE
    AND l.deleted = FALSE
    AND l.lap_time IS NOT NULL
    AND l.stint IS NOT NULL
    AND l.pit_in_time IS NULL
    AND l.pit_out_time IS NULL
