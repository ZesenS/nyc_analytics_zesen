WITH all_times AS (
    SELECT DISTINCT 
        EXTRACT(TIME FROM created_date) AS time_of_day
    FROM {{ ref('stg_311_requests') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT 
        EXTRACT(TIME FROM closed_date) AS time_of_day
    FROM {{ ref('stg_311_requests') }}
    WHERE closed_date IS NOT NULL

    UNION DISTINCT

    -- 补齐秒数为 0，确保类型为 TIME
    SELECT DISTINCT TIME(hour, minute, 0) AS time_of_day
    FROM {{ ref('stg_traffic_volumns') }}
    WHERE hour IS NOT NULL 
    AND minute IS NOT NULL
),

time_dimension AS (
    SELECT
        -- 1. 这里加了一个逗号
        {{ dbt_utils.generate_surrogate_key(['time_of_day']) }} AS time_key, 

        time_of_day,
        EXTRACT(HOUR FROM time_of_day) AS hour,
        EXTRACT(MINUTE FROM time_of_day) AS minute,
        EXTRACT(SECOND FROM time_of_day) AS second
    FROM all_times
)

SELECT * FROM time_dimension