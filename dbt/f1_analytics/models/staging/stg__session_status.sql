WITH session_status AS (
    SELECT * FROM {{ source('f1_data', 'session_status') }}
)

SELECT
    *
FROM session_status
