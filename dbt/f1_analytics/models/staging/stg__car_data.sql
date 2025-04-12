WITH car_data AS (
    SELECT * FROM {{ source('sqlite', 'car_data') }}
)

SELECT
    *
FROM car_data