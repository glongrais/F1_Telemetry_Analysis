WITH
race_results AS (SELECT * FROM {{ ref('int__race_results') }}),
quali_results AS (SELECT * FROM {{ ref('int__qualifying_results') }}),

race_pairs AS (
    SELECT
        r1.session_id,
        r1.year,
        r1.round_number,
        r1.event_name,
        r1.country,
        r1.location,
        r1.team_name,
        r1.team_id,
        r1.driver_number AS driver_1_number,
        r1.driver_code AS driver_1_code,
        r1.full_name AS driver_1_name,
        r1.finish_position AS driver_1_race_position,
        r1.points AS driver_1_race_points,
        r2.driver_number AS driver_2_number,
        r2.driver_code AS driver_2_code,
        r2.full_name AS driver_2_name,
        r2.finish_position AS driver_2_race_position,
        r2.points AS driver_2_race_points,
        CASE
            WHEN r1.finish_position IS NOT NULL AND r2.finish_position IS NOT NULL
            THEN r1.finish_position < r2.finish_position
        END AS driver_1_race_beat
    FROM race_results r1
    INNER JOIN race_results r2
        ON r1.session_id = r2.session_id
        AND r1.team_id = r2.team_id
        AND r1.driver_number < r2.driver_number
),

quali_pairs AS (
    SELECT
        q1.year,
        q1.round_number,
        q1.team_id,
        q1.driver_number AS driver_1_number,
        q1.qualifying_position AS driver_1_quali_position,
        q1.best_qualifying_time AS driver_1_quali_time,
        q2.driver_number AS driver_2_number,
        q2.qualifying_position AS driver_2_quali_position,
        q2.best_qualifying_time AS driver_2_quali_time,
        CASE
            WHEN q1.qualifying_position IS NOT NULL AND q2.qualifying_position IS NOT NULL
            THEN q1.qualifying_position < q2.qualifying_position
        END AS driver_1_quali_beat
    FROM quali_results q1
    INNER JOIN quali_results q2
        ON q1.session_id = q2.session_id
        AND q1.team_id = q2.team_id
        AND q1.driver_number < q2.driver_number
    WHERE q1.session_type = 'Qualifying'
),

combined AS (
    SELECT
        r.session_id,
        COALESCE(r.year, q.year) AS year,
        COALESCE(r.round_number, q.round_number) AS round_number,
        r.event_name,
        r.country,
        r.location,
        r.team_name,
        COALESCE(r.team_id, q.team_id) AS team_id,
        COALESCE(r.driver_1_number, q.driver_1_number) AS driver_1_number,
        r.driver_1_code,
        r.driver_1_name,
        COALESCE(r.driver_2_number, q.driver_2_number) AS driver_2_number,
        r.driver_2_code,
        r.driver_2_name,
        r.driver_1_race_position,
        r.driver_1_race_points,
        r.driver_2_race_position,
        r.driver_2_race_points,
        r.driver_1_race_beat,
        q.driver_1_quali_position,
        q.driver_1_quali_time,
        q.driver_2_quali_position,
        q.driver_2_quali_time,
        q.driver_1_quali_beat
    FROM race_pairs r
    FULL OUTER JOIN quali_pairs q
        ON r.year = q.year
        AND r.round_number = q.round_number
        AND r.team_id = q.team_id
        AND r.driver_1_number = q.driver_1_number
        AND r.driver_2_number = q.driver_2_number
)

SELECT
    *,
    SUM(CASE WHEN driver_1_race_beat THEN 1 ELSE 0 END) OVER (
        PARTITION BY year, team_id, driver_1_number, driver_2_number
        ORDER BY round_number
    ) AS driver_1_race_wins_ytd,
    SUM(CASE WHEN driver_1_race_beat = FALSE THEN 1 ELSE 0 END) OVER (
        PARTITION BY year, team_id, driver_1_number, driver_2_number
        ORDER BY round_number
    ) AS driver_2_race_wins_ytd,
    SUM(CASE WHEN driver_1_quali_beat THEN 1 ELSE 0 END) OVER (
        PARTITION BY year, team_id, driver_1_number, driver_2_number
        ORDER BY round_number
    ) AS driver_1_quali_wins_ytd,
    SUM(CASE WHEN driver_1_quali_beat = FALSE THEN 1 ELSE 0 END) OVER (
        PARTITION BY year, team_id, driver_1_number, driver_2_number
        ORDER BY round_number
    ) AS driver_2_quali_wins_ytd
FROM combined
