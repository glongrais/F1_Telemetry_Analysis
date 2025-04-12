-- f1_analytics/models/staging/stg__positions.sql
WITH
positions AS (SELECT * FROM {{ source('sqlite', 'positions') }})

SELECT
 *
FROM positions