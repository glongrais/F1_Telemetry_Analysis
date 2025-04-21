WITH race_control AS (
    SELECT * FROM {{ source('f1_data', 'race_control') }}
)

SELECT
    *
FROM race_control