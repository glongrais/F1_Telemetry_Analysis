WITH events AS (
    SELECT * FROM {{ source('f1_data', 'events') }}
)

SELECT
    *
FROM events
