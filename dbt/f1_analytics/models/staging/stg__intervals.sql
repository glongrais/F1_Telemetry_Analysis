WITH intervals AS (
    SELECT * FROM {{ source('f1_data', 'intervals') }}
)

SELECT
    *
FROM intervals