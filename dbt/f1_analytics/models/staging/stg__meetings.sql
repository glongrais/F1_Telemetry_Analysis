-- f1_analytics/models/staging/stg__meetings.sql
WITH
meetings AS (SELECT * FROM {{ source('f1_data', 'meetings') }})

SELECT
 *
FROM meetings