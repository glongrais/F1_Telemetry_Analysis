WITH intervals AS (
    SELECT * FROM {{ source('sqlite', 'intervals') }}
)

SELECT
    *
FROM intervals