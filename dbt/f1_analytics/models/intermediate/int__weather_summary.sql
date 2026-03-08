WITH
weather AS (SELECT * FROM {{ ref('stg__weather') }}),
sessions AS (SELECT * FROM {{ ref('stg__sessions') }})

SELECT
    w.session_id,
    s.year,
    s.round_number,
    s.session_type,
    AVG(w.air_temperature) AS avg_air_temp,
    MIN(w.air_temperature) AS min_air_temp,
    MAX(w.air_temperature) AS max_air_temp,
    AVG(w.track_temperature) AS avg_track_temp,
    MIN(w.track_temperature) AS min_track_temp,
    MAX(w.track_temperature) AS max_track_temp,
    AVG(w.humidity) AS avg_humidity,
    AVG(w.pressure) AS avg_pressure,
    BOOL_OR(w.rainfall) AS had_rainfall,
    AVG(CASE WHEN w.rainfall THEN 1.0 ELSE 0.0 END) AS rainfall_fraction,
    AVG(w.wind_speed) AS avg_wind_speed,
    MAX(w.wind_speed) AS max_wind_speed,
    CASE
        WHEN AVG(CASE WHEN w.rainfall THEN 1.0 ELSE 0.0 END) > 0.1 THEN 'WET'
        ELSE 'DRY'
    END AS session_weather_class
FROM weather w
INNER JOIN sessions s ON w.session_id = s.session_id
GROUP BY w.session_id, s.year, s.round_number, s.session_type
