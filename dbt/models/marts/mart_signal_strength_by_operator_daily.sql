-- Daily signal per postal code and operator, weighted by readings.
-- sum/count instead of avg(average_signal): an hour with 1 reading no longer
-- counts as much as an hour with 20.
with staged as (
    select * from {{ ref('stg_signal_strength_by_operator') }}
)
select
    process_date,
    postal_code,
    operator,
    sum(sum_signal) / nullif(sum(count_signal), 0) as daily_average_signal,
    sum(count_signal)                              as readings,
    count(distinct partition_hour)                 as hours_represented
from staged
group by process_date, postal_code, operator