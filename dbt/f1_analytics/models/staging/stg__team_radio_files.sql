-- dbt/f1_analytics/models/staging/stg__team_radio_files.sql

WITH
team_radio_files AS (SELECT * FROM {{ source('f1_data', 'team_radio_files') }})

SELECT
    *
FROM team_radio_files