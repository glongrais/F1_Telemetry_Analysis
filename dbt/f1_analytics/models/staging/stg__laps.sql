-- f1_analytics/models/staging/stg__laps.sql
WITH
laps AS (SELECT * FROM {{ source('sqlite', 'laps') }})

SELECT
 *
FROM laps