WITH source AS (
    SELECT * FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
), 

cleaned AS (
    SELECT
        * EXCEPT (
            bin,
            bulding_number,
            council_district,
            census_tract,
            community_board,
            longitude,
            latitude,
            qualify_alcohol,
            roadway_dimensions_length,
            roadway_dimensions_width,
            sidewalk_dimensions_area,
            sidewalk_dimensions_length,
            sidewalk_dimensions_width,
            time_of_submission,
            healthcompliance_terms,
            landmark_district_or_building,
            globalid,
            landmarkdistrict_terms
        ),

        -- 1. Rename
        bulding_number AS building_number,

        -- 2. Date
        CAST(time_of_submission AS TIMESTAMP) AS time_of_submission,

        -- 3. Decimal
        CAST(latitude AS DECIMAL) AS latitude,
        CAST(longitude AS DECIMAL) AS longitude,

        -- 4. Bool
        CASE 
            WHEN UPPER(TRIM(qualify_alcohol)) = 'YES' THEN TRUE
            WHEN UPPER(TRIM(qualify_alcohol)) = 'NO' THEN FALSE
            ELSE NULL 
        END AS has_alcohol,

        CASE 
            WHEN UPPER(TRIM(healthcompliance_terms)) = 'YES' THEN TRUE
            WHEN UPPER(TRIM(healthcompliance_terms)) = 'NO' THEN FALSE
            ELSE NULL 
        END AS healthcompliance_terms,

        CASE 
            WHEN UPPER(TRIM(landmark_district_or_building)) = 'YES' THEN TRUE
            WHEN UPPER(TRIM(landmark_district_or_building)) = 'NO' THEN FALSE
            ELSE NULL 
        END AS landmark_district_or_building,

        CASE 
            WHEN UPPER(TRIM(landmarkdistrict_terms)) = 'YES' THEN TRUE
            WHEN UPPER(TRIM(landmarkdistrict_terms)) = 'NO' THEN FALSE
            ELSE NULL 
        END AS landmarkdistrict_terms,

        -- 5. Replace {} in globalid
        REPLACE(REPLACE(globalid, '{', ''), '}', '') AS globalid,

        -- 6. Integer
        CAST(bin AS INTEGER) AS bin,
        CAST(census_tract AS INTEGER) AS census_tract,
        CAST(community_board AS INTEGER) AS community_board,
        CAST(council_district AS INTEGER) AS council_district,
        CAST(roadway_dimensions_length AS INTEGER) AS roadway_dimensions_length,
        CAST(roadway_dimensions_width AS INTEGER) AS roadway_dimensions_width,
        CAST(sidewalk_dimensions_area AS INTEGER) AS sidewalk_dimensions_area,
        CAST(sidewalk_dimensions_length AS INTEGER) AS sidewalk_dimensions_length,
        CAST(sidewalk_dimensions_width AS INTEGER) AS sidewalk_dimensions_width
    -- 7. Filter
    FROM source
    WHERE objectid IS NOT NULL

   -- 7. Duplicate
   QUALIFY ROW_NUMBER() OVER (PARTITION BY objectid ORDER BY time_of_submission DESC) = 1
)


SELECT * FROM cleaned