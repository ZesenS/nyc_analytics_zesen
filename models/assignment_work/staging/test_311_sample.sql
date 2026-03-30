WITH all_locations AS (
   -- Get locations from 311 requests
   SELECT DISTINCT
      incident_zip AS zipcode,
      borough
   FROM {{ ref('stg_nyc_311_dot') }}
   WHERE borough IS NOT NULL

   UNION DISTINCT

   -- Get locations from restaurant applications
   SELECT DISTINCT
      zip AS zipcode,
      borough
   FROM {{ ref('stg_nyc_open_restaurant_apps') }}
   WHERE borough IS NOT NULL
),

location_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key(['borough', 'zipcode']) }} AS location_key,
       borough,
       zipcode
   FROM all_locations
)

SELECT * FROM location_dimension --TODO replace ??s with what to select. HINT: May be quite simple!