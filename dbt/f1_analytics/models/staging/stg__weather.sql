WITH weather AS (
    SELECT * FROM {{ source('sqlite', 'weather') }}
)

SELECT
    *
FROM weather