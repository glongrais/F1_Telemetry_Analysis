WITH sessions AS (
    SELECT * FROM {{ source('f1_data', 'sessions') }}
)

SELECT
    *
FROM sessions
