WITH pits AS (
    SELECT * FROM {{ source('f1_data', 'pits') }}
)

SELECT
    *
FROM pits