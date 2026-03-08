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
    d.driver_number,
    d.driver_code,
    d.full_name,
    d.team_name,
    d.team_id,
    d.grid_position,
    d.position AS finish_position,
    d.classified_position,
    d.status,
    d.points,
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
WHERE s.session_type = 'Race'
