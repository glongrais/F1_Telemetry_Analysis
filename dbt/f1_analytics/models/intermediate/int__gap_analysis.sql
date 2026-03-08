WITH
intervals AS (SELECT * FROM {{ ref('stg__intervals') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }})

SELECT
    i.session_id,
    s.year,
    s.round_number,
    s.session_type,
    i.driver_number,
    i.timestamp,
    i.position,
    i.gap_to_leader,
    TRY_CAST(i.gap_to_leader AS DOUBLE) AS gap_to_leader_seconds,
    i.interval_to_ahead,
    TRY_CAST(i.interval_to_ahead AS DOUBLE) AS interval_to_ahead_seconds,
    LAG(i.position) OVER (
        PARTITION BY i.session_id, i.driver_number
        ORDER BY i.timestamp
    ) AS prev_position,
    i.position - LAG(i.position) OVER (
        PARTITION BY i.session_id, i.driver_number
        ORDER BY i.timestamp
    ) AS position_change,
    LAG(TRY_CAST(i.gap_to_leader AS DOUBLE)) OVER (
        PARTITION BY i.session_id, i.driver_number
        ORDER BY i.timestamp
    ) AS prev_gap_to_leader,
    TRY_CAST(i.gap_to_leader AS DOUBLE)
        - LAG(TRY_CAST(i.gap_to_leader AS DOUBLE)) OVER (
            PARTITION BY i.session_id, i.driver_number
            ORDER BY i.timestamp
        )
    AS gap_to_leader_delta
FROM intervals i
INNER JOIN sessions s ON i.session_id = s.session_id
