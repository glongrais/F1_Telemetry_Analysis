WITH drivers AS (
    SELECT * FROM {{ source('sqlite', 'drivers') }}
)

SELECT
    *
FROM drivers