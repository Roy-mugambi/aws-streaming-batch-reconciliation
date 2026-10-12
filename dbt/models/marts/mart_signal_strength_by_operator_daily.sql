with staged as (
    select * from {{ ref('stg_signal_strength_by_operator') }}
)
select
    process_date,
    postal_code,
    operator,
    avg(average_signal) as daily_average_signal,
    count(distinct partition_hour) as hours_represented
from staged
group by process_date, postal_code, operator