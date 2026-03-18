with source as (
    select * from {{ source('traffic_raw_source', 'group_3_source_traffic_volume_history') }}
)

select * from source