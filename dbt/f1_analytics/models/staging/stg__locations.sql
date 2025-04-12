WITH locations AS (
    SELECT * FROM {{ source('sqlite', 'locations') }}
)

SELECT
    *
FROM locations