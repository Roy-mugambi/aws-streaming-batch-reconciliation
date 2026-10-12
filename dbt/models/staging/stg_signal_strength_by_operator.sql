with source as (
    SELECT * FROM {{ source('staging_raw', 'raw_signal_strength_by_operator') }}
)
select 
    process_date,
    partition_hour::int as partition_hour,
    postal_code,
    operator,
    average_signal,
    _file_name,
    _loaded_at
from source
where partition_hour is not NULL
