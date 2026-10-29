-- Daily GPS quality per provider, weighted by readings.
with staged as (
    select * from {{ ref('stg_gps_precision_by_provider') }}
)
select
    process_date,
    provider,
    sum(sum_precision)  / nullif(sum(count_precision), 0)  as daily_average_precision,
    sum(sum_satellites) / nullif(sum(count_satellites), 0) as daily_average_satellites,
    sum(count_precision)                                   as readings,
    count(distinct partition_hour)                         as hours_represented
from staged
group by process_date, provider