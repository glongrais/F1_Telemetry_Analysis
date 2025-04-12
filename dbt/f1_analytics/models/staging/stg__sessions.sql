-- f1_analytics/models/staging/stg__sessions.sql
WITH
sessions AS (SELECT * FROM {{ source('sqlite', 'sessions') }})

SELECT
 *
FROM sessions