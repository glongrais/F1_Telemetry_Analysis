WITH stints AS (
    SELECT * FROM {{ source('sqlite', 'stints') }}
)

SELECT
    *
FROM stints