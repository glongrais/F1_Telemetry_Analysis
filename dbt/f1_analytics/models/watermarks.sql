-- f1_analytics/models/watermarks.sql
WITH
last_meetings AS (SELECT 'meetings' AS table_name, MAX(meeting_key) AS meeting_key, NULL AS session_key FROM {{ ref('stg__meetings') }}),
last_sessions AS (SELECT 'sessions' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__sessions') }}),
last_laps AS (SELECT 'laps' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__laps') }}),
last_positions AS (SELECT 'positions' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__positions') }}),
last_car_data AS (SELECT 'car_data' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__car_data') }}),
last_drivers AS (SELECT 'drivers' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__drivers') }}),
last_intervals AS (SELECT 'intervals' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__intervals') }}),
last_locations AS (SELECT 'locations' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__locations') }}),
last_pits AS (SELECT 'pits' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__pits') }}),
last_race_control AS (SELECT 'race_control' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__race_control') }}),
last_stints AS (SELECT 'stints' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__stints') }}),
last_team_radio AS (SELECT 'team_radio' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__team_radio') }}),
last_weather AS (SELECT 'weather' AS table_name, MAX(meeting_key) AS meeting_key, MAX(session_key) AS session_key FROM {{ ref('stg__weather') }}),

combined_keys AS (
  SELECT
    table_name,
    meeting_key,
    session_key
  FROM last_meetings
  UNION ALL SELECT * FROM last_sessions
  UNION ALL SELECT * FROM last_laps
  UNION ALL SELECT * FROM last_positions
  UNION ALL SELECT * FROM last_car_data
  UNION ALL SELECT * FROM last_drivers
  UNION ALL SELECT * FROM last_intervals
  UNION ALL SELECT * FROM last_locations
  UNION ALL SELECT * FROM last_pits
  UNION ALL SELECT * FROM last_race_control
  UNION ALL SELECT * FROM last_stints
  UNION ALL SELECT * FROM last_team_radio
  UNION ALL SELECT * FROM last_weather
)

SELECT
  table_name,
  COALESCE(meeting_key, 0) AS meeting_key,
  COALESCE(session_key, 0) AS session_key
FROM combined_keys