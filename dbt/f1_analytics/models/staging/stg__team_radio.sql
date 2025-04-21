WITH team_radio AS (
    SELECT * FROM {{ source('f1_data', 'team_radio') }}
)

SELECT
    *
FROM team_radio