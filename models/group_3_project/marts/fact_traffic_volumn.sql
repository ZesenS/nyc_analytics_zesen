{{ config(schema='group_3_marts') }}

WITH traffic_staging AS (
    SELECT * FROM {{ ref('stg_traffic_volumns') }}
)

SELECT
    traffic_request_id AS requestid,


    {{ dbt_utils.generate_surrogate_key(["DATE(year, month, day)"]) }} AS date_id,
    {{ dbt_utils.generate_surrogate_key(["TIME(hour, minute, 0)"]) }} AS time_id,
    {{ dbt_utils.generate_surrogate_key(['borough']) }} AS location_id,

    {{ dbt_utils.generate_surrogate_key(['street_name', 'from_street', 'to_street', 'direction']) }} AS segment_id,


    longitude,
    latitude,
    traffic_volume AS vol

FROM traffic_staging
WHERE traffic_volume IS NOT NULL