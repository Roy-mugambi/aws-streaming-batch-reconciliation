with source as ( 
    SELECT * from {{source('staging_raw','raw_gps_precision_by_provider')}}

)
SELECT
    process_date,
    partition_hour::int as partition_hour,
    provider,
    average_precision,
    average_satellites,
from source
where partition_hour is not null