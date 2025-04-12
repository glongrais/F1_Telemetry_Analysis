-- f1_analytics/models/staging/stg__meetings.sql
WITH
meetings AS (SELECT * FROM {{ source('sqlite', 'meetings') }})

SELECT
 *
FROM meetings