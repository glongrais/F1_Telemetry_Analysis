WITH laps AS (
    SELECT * FROM {{ source('f1_data', 'laps') }}
)

SELECT
    *
FROM laps
