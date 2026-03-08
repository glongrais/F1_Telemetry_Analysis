WITH laps AS (
    SELECT * FROM {{ ref('stg__laps') }}
)

SELECT
    session_id,
    driver_number,
    lap_number,
    pit_in_time,
    pit_out_time
FROM laps
WHERE pit_in_time IS NOT NULL OR pit_out_time IS NOT NULL
ORDER BY session_id, driver_number, lap_number
