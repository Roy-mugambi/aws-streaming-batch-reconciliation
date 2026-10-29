{{ config(materialized='table') }}
 
-- One row per (event_date, postal_code, operator): readings and weighted
-- average signal from each lane. Both sides sum sum_signal and count_signal,
-- so the averages are exact regardless of window or hour grain.
 
 with streaming_days as (
    select distinct to_date(window_start) as d
    from {{ source('staging_raw', 'ext_streaming_signal_strength_by_operator') }}
),

batch as (
    select
        process_date            as event_date,
        postal_code,
        operator,
        sum(sum_signal)         as batch_sum_signal,
        sum(count_signal)       as batch_readings
    from {{ source('staging_raw', 'raw_signal_strength_by_operator') }}
    where count_signal is not null
      and process_date in (select d from streaming_days)
    group by 1, 2, 3
),
 
streaming as (
    select
        to_date(window_start)   as event_date,
        postal_code,
        operator,
        sum(sum_signal)         as streaming_sum_signal,
        sum(count_signal)       as streaming_readings
    from {{ source('staging_raw', 'ext_streaming_signal_strength_by_operator') }}
    where to_date(window_start) in (select distinct event_date from batch)
    group by 1, 2, 3
),
 
joined as (
    select
        coalesce(b.event_date, s.event_date)    as event_date,
        coalesce(b.postal_code, s.postal_code)  as postal_code,
        coalesce(b.operator, s.operator)        as operator,
        coalesce(b.batch_readings, 0)           as batch_readings,
        coalesce(s.streaming_readings, 0)       as streaming_readings,
        b.batch_sum_signal / nullif(b.batch_readings, 0)             as batch_avg_signal,
        s.streaming_sum_signal / nullif(s.streaming_readings, 0)     as streaming_avg_signal
    from batch b
    full outer join streaming s
        on  b.event_date  = s.event_date
        and b.postal_code = s.postal_code
        and b.operator    = s.operator
)
 
select
    *,
    batch_readings - streaming_readings                  as reading_gap,
    round(batch_avg_signal - streaming_avg_signal, 4)    as avg_signal_diff,
    case
        when streaming_readings = 0 then 'missing_in_streaming'
        when batch_readings = 0     then 'missing_in_batch'
        when batch_readings = streaming_readings
         and abs(batch_avg_signal - streaming_avg_signal) < 0.0001 then 'match'
        else 'mismatch'
    end                                                  as recon_status,
    current_timestamp()                                  as reconciled_at
from joined
 