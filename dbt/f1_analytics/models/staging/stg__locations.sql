WITH locations AS (
    SELECT * FROM {{ source('f1_data', 'locations') }}
)

SELECT
    *
FROM locations