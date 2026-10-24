
{{ config(materialized='table') }}
 
-- One row per day: the "station end-of-day sheet".
select
    event_date,
    sum(batch_events)                                         as batch_events,
    sum(streaming_events)                                     as streaming_events,
    sum(event_gap)                                            as event_gap,
    round(100 * sum(event_gap) / nullif(sum(batch_events), 0), 2) as gap_pct,
    count_if(recon_status <> 'match')                         as cells_out_of_balance,
    count(*)                                                  as cells_compared
from {{ ref('recon_status_count_daily') }}
group by 1