-- f1_analytics/models/staging/stg__sessions.sql
WITH
sessions AS (SELECT * FROM {{ source('f1_data', 'sessions') }})

SELECT
 *
FROM sessions