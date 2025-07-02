-- dbt/f1_analytics/models/radios_watermark.sql

WITH
team_radio_files AS (SELECT * FROM {{ ref('stg__team_radio_files') }}),
team_radio_texts AS (SELECT * FROM {{ ref('stg__team_radio_texts') }})

SELECT
    *
FROM team_radio_files
WHERE
 team_radio_files.team_radio_file_id NOT IN (SELECT team_radio_file_id FROM team_radio_texts)