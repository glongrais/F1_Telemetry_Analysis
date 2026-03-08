WITH track_status AS (
    SELECT * FROM {{ source('f1_data', 'track_status') }}
)

SELECT
    *
FROM track_status
