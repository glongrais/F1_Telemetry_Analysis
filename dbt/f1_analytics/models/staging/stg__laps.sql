-- f1_analytics/models/staging/stg__laps.sql
WITH
laps AS (SELECT * FROM {{ source('f1_data', 'laps') }})

SELECT
 *
FROM laps