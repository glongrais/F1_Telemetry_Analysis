WITH
driver_standings AS (SELECT * FROM {{ ref('mart__driver_standings') }}),

team_round AS (
    SELECT
        year,
        round_number,
        event_name,
        country,
        location,
        team_name,
        team_id,
        SUM(points) AS team_race_points,
        SUM(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS team_wins_this_round,
        MIN(finish_position) AS best_finish,
        SUM(CASE WHEN is_dnf THEN 1 ELSE 0 END) AS dnfs
    FROM driver_standings
    GROUP BY year, round_number, event_name, country, location, team_name, team_id
),

cumulative AS (
    SELECT
        *,
        SUM(team_race_points) OVER (
            PARTITION BY year, team_id
            ORDER BY round_number
        ) AS team_cumulative_points,
        SUM(team_wins_this_round) OVER (
            PARTITION BY year, team_id
            ORDER BY round_number
        ) AS team_cumulative_wins
    FROM team_round
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY year, round_number
        ORDER BY team_cumulative_points DESC, team_cumulative_wins DESC
    ) AS constructor_championship_position
FROM cumulative
