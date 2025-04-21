WITH stints AS (
    SELECT * FROM {{ source('f1_data', 'stints') }}
)

SELECT
    *
FROM stints