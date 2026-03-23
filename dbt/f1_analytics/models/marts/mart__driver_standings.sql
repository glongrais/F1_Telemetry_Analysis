WITH
race_results AS (SELECT * FROM {{ ref('int__race_results') }}),

round_summary AS (
    SELECT
        MAX(CASE WHEN session_type = 'Race' THEN session_id END) AS session_id,
        year,
        round_number,
        MAX(event_name) AS event_name,
        MAX(country) AS country,
        MAX(location) AS location,
        driver_number,
        MAX(driver_code) AS driver_code,
        MAX(full_name) AS full_name,
        MAX(team_name) AS team_name,
        MAX(team_id) AS team_id,
        MAX(CASE WHEN session_type = 'Race' THEN finish_position END) AS finish_position,
        SUM(points) AS points,
        MAX(CASE WHEN session_type = 'Race' THEN is_dnf END) AS is_dnf,
        MAX(CASE WHEN session_type = 'Race' THEN positions_gained END) AS positions_gained
    FROM race_results
    GROUP BY year, round_number, driver_number
),

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
    FROM round_summary
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY year, round_number
        ORDER BY cumulative_points DESC, cumulative_wins DESC
    ) AS championship_position
FROM cumulative
