with staged as(
    select * from {{ref('stg_gps_precision_by_provider')}}
)
select 
    process_date,
    provider,
    avg(average_precision) as daily_average_precision,
    avg(average_satellites) as daily_average_satellites,
    count(distinct partition_hour) as hours_represented
from staged
group by process_date, provider
