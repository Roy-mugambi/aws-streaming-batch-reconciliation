{{ config(materialized='table') }}
 
-- One row per (event_date, postal_code, status): batch events vs streaming
-- events for the same replay day. Streaming 2-minute windows are rolled up to
-- the day so both sides share one grain.
--
-- The event date: batch takes it from the Firehose arrival folder (date=...),
-- streaming from current_date() at processing time (inside window_start).
-- Both agree only when one producer replay feeds both pipelines on one day.
 
with batch as (
    select
        process_date              as event_date,
        postal_code,
        status::string            as status,
        sum(status_count)         as batch_events
    from {{ source('staging_raw', 'raw_status_count') }}
    group by 1, 2, 3
),
 
streaming as (
    select
        to_date(window_start)     as event_date,
        postal_code,
        status::string            as status,
        sum(status_count)         as streaming_events
    from {{ source('staging_raw', 'ext_streaming_status_count') }}
    -- only days the batch side has processed; other replays are out of scope
    where to_date(window_start) in (select distinct event_date from batch)
    group by 1, 2, 3
),
 
joined as (
    select
        coalesce(b.event_date, s.event_date)    as event_date,
        coalesce(b.postal_code, s.postal_code)  as postal_code,
        coalesce(b.status, s.status)            as status,
        coalesce(b.batch_events, 0)             as batch_events,
        coalesce(s.streaming_events, 0)         as streaming_events
    from batch b
    full outer join streaming s
        on  b.event_date  = s.event_date
        and b.postal_code = s.postal_code
        and b.status      = s.status
)
 
select
    *,
    batch_events - streaming_events             as event_gap,
    case
        when batch_events = streaming_events then 'match'
        when streaming_events = 0            then 'missing_in_streaming'
        when batch_events = 0                then 'missing_in_batch'
        else 'count_mismatch'
    end                                         as recon_status,
    current_timestamp()                         as reconciled_at
from joined