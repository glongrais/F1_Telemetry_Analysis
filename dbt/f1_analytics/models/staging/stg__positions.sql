-- f1_analytics/models/staging/stg__positions.sql
WITH
positions AS (SELECT * FROM {{ source('f1_data', 'positions') }})

SELECT
 *
FROM positions