-- dbt/f1_analytics/models/staging/stg__team_radio_texts.sql

WITH
team_radio_texts AS (SELECT * FROM {{ source('f1_data', 'team_radio_texts') }})

SELECT
    *
FROM team_radio_texts