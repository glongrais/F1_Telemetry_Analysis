WITH
race_results AS (SELECT * FROM {{ ref('int__race_results') }}),

cumulative AS (
    SELECT
        session_id,
        year,
        round_number,
        event_name,
        country,
        location,
        driver_number,
        driver_code,
        full_name,
        team_name,
        team_id,
        finish_position,
        points,
        is_dnf,
        positions_gained,
        SUM(points) OVER (
            PARTITION BY year, driver_number
            ORDER BY round_number
        ) AS cumulative_points,
        SUM(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY year, driver_number
            ORDER BY round_number
        ) AS cumulative_wins,
        SUM(CASE WHEN finish_position <= 3 THEN 1 ELSE 0 END) OVER (
            PARTITION BY year, driver_number
            ORDER BY round_number
        ) AS cumulative_podiums,
        SUM(CASE WHEN is_dnf THEN 1 ELSE 0 END) OVER (
            PARTITION BY year, driver_number
            ORDER BY round_number
        ) AS cumulative_dnfs
    FROM race_results
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY year, round_number
        ORDER BY cumulative_points DESC, cumulative_wins DESC
    ) AS championship_position
FROM cumulative
