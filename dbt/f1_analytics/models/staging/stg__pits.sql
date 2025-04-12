WITH pits AS (
    SELECT * FROM {{ source('sqlite', 'pits') }}
)

SELECT
    *
FROM pits