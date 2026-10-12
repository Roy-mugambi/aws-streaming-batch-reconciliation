with source as ( 
    SELECT * from {{ source('staging_raw', 'raw_status_count') }}

)
select 
    process_date,
    partition_hour::int as partition_hour,
    postal_code,
    status,
    status_count,
from source
where partition_hour is not null