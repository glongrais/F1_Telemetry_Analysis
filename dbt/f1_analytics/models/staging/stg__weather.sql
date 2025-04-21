WITH weather AS (
    SELECT * FROM {{ source('f1_data', 'weather') }}
)

SELECT
    *
FROM weather