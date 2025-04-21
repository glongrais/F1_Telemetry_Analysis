WITH
laps AS (SELECT * FROM {{ ref('stg__laps') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }}),

laps_session_type AS (
    SELECT
        l.*,
        s.session_type
    FROM laps l
    LEFT JOIN sessions s ON l.session_key = s.session_key
),

fastest_laps AS (
    SELECT
        session_key,
        min(lap_duration) AS fastest_lap_time
    FROM laps_session_type
    WHERE session_type == 'Race'
    GROUP BY session_key
)

SELECT
    l.*,
    f.fastest_lap_time
FROM fastest_laps f
LEFT JOIN laps l
    ON l.session_key = f.session_key
    AND l.lap_duration = f.fastest_lap_time
