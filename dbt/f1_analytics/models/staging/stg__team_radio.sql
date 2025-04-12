WITH team_radio AS (
    SELECT * FROM {{ source('sqlite', 'team_radio') }}
)

SELECT
    *
FROM team_radio