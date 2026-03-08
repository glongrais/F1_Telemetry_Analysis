WITH
laps AS (SELECT * FROM {{ ref('stg__laps') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }}),

laps_session_type AS (
    SELECT
        l.*,
        s.session_type
    FROM laps l
    LEFT JOIN sessions s ON l.session_id = s.session_id
),

fastest_laps AS (
    SELECT
        session_id,
        min(lap_time) AS fastest_lap_time
    FROM laps_session_type
    WHERE session_type = 'Race'
    GROUP BY session_id
)

SELECT
    l.*,
    f.fastest_lap_time
FROM fastest_laps f
LEFT JOIN laps l
    ON l.session_id = f.session_id
    AND l.lap_time = f.fastest_lap_time
