with source as (
    select * from {{ source('staging_raw', 'raw_gps_precision_by_provider') }}
)
select
    process_date,
    partition_hour::int as partition_hour,
    provider,
    average_precision,
    average_satellites,
    sum_precision,
    count_precision,
    sum_satellites,
    count_satellites
from source
where partition_hour is not null