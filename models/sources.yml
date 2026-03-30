{{ config(materialized='table') }}

WITH staging AS (
    SELECT 
        *,
        CAST(time_of_submission AS DATE) AS submission_date,

        CASE
            WHEN seating_interest_sidewalk IN ('both', 'roadway') THEN TRUE
            ELSE FALSE
        END AS approved_for_roadway,

        CASE
            WHEN seating_interest_sidewalk IN ('both', 'sidewalk') THEN TRUE
            ELSE FALSE
        END AS approved_for_sidewalk
        
    FROM {{ ref('stg_nyc_open_restaurant_apps') }}
)

SELECT 
    * EXCEPT(
        zip, 
        borough, 
        time_of_submission, 
        submission_date,
        seating_interest_sidewalk,
        approved_for_roadway,
        approved_for_sidewalk
    ),

    {{ dbt_utils.generate_surrogate_key([
        'seating_interest_sidewalk',
        'approved_for_sidewalk',
        'approved_for_roadway'
    ]) }} AS seating_type_key,

    {{ dbt_utils.generate_surrogate_key(['borough', 'zip']) }} AS location_key,

    {{ dbt_utils.generate_surrogate_key(['submission_date']) }} AS date_key

FROM staging