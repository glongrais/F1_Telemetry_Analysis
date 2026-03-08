WITH
race_control AS (SELECT * FROM {{ ref('stg__race_control') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }})

SELECT
    rc.race_control_id,
    rc.session_id,
    s.year,
    s.round_number,
    s.session_type,
    rc.timestamp,
    rc.category,
    rc.message,
    rc.status,
    rc.flag,
    rc.scope,
    rc.sector,
    rc.driver_number,
    rc.lap_number,
    CASE
        WHEN rc.category = 'SafetyCar' AND rc.message LIKE '%VIRTUAL%' THEN 'VIRTUAL_SAFETY_CAR'
        WHEN rc.category = 'SafetyCar' THEN 'SAFETY_CAR'
        WHEN rc.flag = 'RED' THEN 'RED_FLAG'
        WHEN rc.category = 'Drs' AND rc.message LIKE '%ENABLED%' THEN 'DRS_ENABLED'
        WHEN rc.category = 'Drs' AND rc.message LIKE '%DISABLED%' THEN 'DRS_DISABLED'
        WHEN rc.message LIKE '%PENALTY%' THEN 'PENALTY'
        WHEN rc.message LIKE '%INVESTIGATION%' OR rc.message LIKE '%NOTED%' THEN 'INVESTIGATION'
        WHEN rc.message LIKE '%TRACK LIMITS%' THEN 'TRACK_LIMITS'
        WHEN rc.message LIKE '%WARNING%' OR rc.flag = 'BLACK AND WHITE' THEN 'WARNING'
        WHEN rc.flag = 'YELLOW' OR rc.flag = 'DOUBLE YELLOW' THEN 'YELLOW_FLAG'
        WHEN rc.flag = 'GREEN' OR rc.flag = 'CLEAR' THEN 'GREEN_FLAG'
        WHEN rc.flag = 'CHEQUERED' THEN 'CHEQUERED_FLAG'
        WHEN rc.flag = 'BLUE' THEN 'BLUE_FLAG'
        ELSE 'OTHER'
    END AS incident_type,
    CASE
        WHEN rc.category = 'SafetyCar' THEN TRUE
        WHEN rc.flag = 'RED' THEN TRUE
        ELSE FALSE
    END AS is_neutralization,
    CASE
        WHEN rc.message LIKE '%PENALTY%'
        THEN TRY_CAST(
            regexp_extract(rc.message, '(\d+)\s+SECOND', 1) AS INTEGER
        )
    END AS penalty_seconds
FROM race_control rc
INNER JOIN sessions s ON rc.session_id = s.session_id
