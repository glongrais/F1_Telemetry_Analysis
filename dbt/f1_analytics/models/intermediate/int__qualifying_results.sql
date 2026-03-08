WITH
drivers AS (SELECT * FROM {{ ref('stg__drivers') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }}),
events AS (SELECT * FROM {{ ref('stg__events') }})

SELECT
    d.session_id,
    s.year,
    s.round_number,
    e.event_name,
    e.country,
    e.location,
    s.session_type,
    d.driver_number,
    d.driver_code,
    d.full_name,
    d.team_name,
    d.team_id,
    d.position AS qualifying_position,
    d.q1,
    d.q2,
    d.q3,
    COALESCE(d.q3, d.q2, d.q1) AS best_qualifying_time,
    CASE
        WHEN d.q3 IS NOT NULL THEN 'Q3'
        WHEN d.q2 IS NOT NULL THEN 'Q2'
        WHEN d.q1 IS NOT NULL THEN 'Q1'
    END AS furthest_session,
    COALESCE(d.q3, d.q2, d.q1)
        - MIN(COALESCE(d.q3, d.q2, d.q1)) OVER (PARTITION BY d.session_id)
    AS gap_to_pole
FROM drivers d
INNER JOIN sessions s ON d.session_id = s.session_id
INNER JOIN events e ON s.event_id = e.event_id
WHERE s.session_type IN ('Qualifying', 'Sprint Qualifying', 'Sprint Shootout')
