{{ config(materialized='table') }}

WITH staging AS (
    SELECT 
        *,
        CAST(created_date AS DATE) AS submission_date
    FROM {{ ref('stg_nyc_311_dot') }}
)

SELECT 
    * EXCEPT(
        incident_zip, 
        borough, 
        created_date,
        submission_date
    ),

    {{ dbt_utils.generate_surrogate_key(['borough', 'incident_zip']) }} AS location_key,

    {{ dbt_utils.generate_surrogate_key(['submission_date']) }} AS date_key

FROM staging