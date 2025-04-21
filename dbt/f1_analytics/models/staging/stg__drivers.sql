WITH drivers AS (
    SELECT * FROM {{ source('f1_data', 'drivers') }}
)

SELECT
    *
FROM drivers