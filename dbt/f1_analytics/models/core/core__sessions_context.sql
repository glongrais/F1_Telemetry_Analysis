-- f1_analytics/models/core/core__sessions_context.sql
WITH
sessions AS (SELECT * FROM {{ ref('stg__sessions') }}),
drivers AS (SELECT * FROM {{ ref('stg__drivers') }}),

drivers_per_session AS (
    SELECT
        d.session_key,
        GROUP_CONCAT(d.driver_number, ', ') AS driver_numbers
    FROM drivers d
    GROUP BY d.session_key
)

SELECT
    s.session_key,
    s.date_start,
    s.date_end,
    s.session_type,
    COALESCE(dps.driver_numbers, '') AS driver_numbers
FROM sessions s
LEFT JOIN drivers_per_session dps
    ON s.session_key = dps.session_key
