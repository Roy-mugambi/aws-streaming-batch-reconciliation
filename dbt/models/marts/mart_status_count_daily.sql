with staged as (
    select * from {{ ref('stg_status_count') }}
)
select
    process_date,
    postal_code,
    status,
    sum(status_count) as daily_status_count,
    count(distinct partition_hour) as hours_represented
from staged
group by process_date, postal_code, status