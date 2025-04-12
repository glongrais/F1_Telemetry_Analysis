WITH race_control AS (
    SELECT * FROM {{ source('sqlite', 'race_control') }}
)

SELECT
    *
FROM race_control