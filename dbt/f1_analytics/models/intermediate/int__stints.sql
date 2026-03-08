WITH laps AS (
    SELECT * FROM {{ ref('stg__laps') }}
)

SELECT
    session_id,
    driver_number,
    stint,
    compound,
    MIN(lap_number) AS lap_start,
    MAX(lap_number) AS lap_end,
    COUNT(*) AS lap_count,
    MIN(tyre_life) AS tyre_age_at_start,
    MAX(tyre_life) AS tyre_age_at_end
FROM laps
WHERE stint IS NOT NULL
GROUP BY session_id, driver_number, stint, compound
ORDER BY session_id, driver_number, stint
