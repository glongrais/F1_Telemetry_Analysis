WITH car_data AS (
    SELECT * FROM {{ source('f1_data', 'car_data') }}
)

SELECT
    *
FROM car_data