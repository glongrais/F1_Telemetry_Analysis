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
    d.grid_position,
    d.position AS finish_position,
    d.classified_position,
    d.status,
    COALESCE(d.points, CASE
        WHEN s.session_type = 'Race' THEN CASE d.position
            WHEN 1 THEN 25 WHEN 2 THEN 18 WHEN 3 THEN 15 WHEN 4 THEN 12
            WHEN 5 THEN 10 WHEN 6 THEN 8 WHEN 7 THEN 6 WHEN 8 THEN 4
            WHEN 9 THEN 2 WHEN 10 THEN 1 ELSE 0 END
        WHEN s.session_type = 'Sprint' THEN CASE d.position
            WHEN 1 THEN 8 WHEN 2 THEN 7 WHEN 3 THEN 6 WHEN 4 THEN 5
            WHEN 5 THEN 4 WHEN 6 THEN 3 WHEN 7 THEN 2 WHEN 8 THEN 1
            ELSE 0 END
    END) AS points,
    d.laps_completed,
    d.finish_time,
    s.total_laps,
    CASE
        WHEN d.grid_position IS NOT NULL
            AND d.grid_position > 0
            AND d.position IS NOT NULL
        THEN d.grid_position - d.position
    END AS positions_gained,
    CASE
        WHEN d.status NOT IN ('Finished', 'Lapped')
            AND d.classified_position IS NULL
        THEN TRUE
        ELSE FALSE
    END AS is_dnf
FROM drivers d
INNER JOIN sessions s ON d.session_id = s.session_id
INNER JOIN events e ON s.event_id = e.event_id
WHERE s.session_type IN ('Race', 'Sprint')
