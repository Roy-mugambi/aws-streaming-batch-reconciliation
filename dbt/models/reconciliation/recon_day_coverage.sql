{{ config(materialized='table') }}
-- Which days each lane has, per dataset. A day in only one lane is a coverage gap, not a mismatch.
with s_b as (select distinct process_date d from {{ source('staging_raw','raw_status_count') }}),
     s_s as (select distinct to_date(window_start) d from {{ source('staging_raw','ext_streaming_status_count') }}),
     g_b as (select distinct process_date d from {{ source('staging_raw','raw_signal_strength_by_operator') }} where count_signal is not null),
     g_s as (select distinct to_date(window_start) d from {{ source('staging_raw','ext_streaming_signal_strength_by_operator') }})
select 'status_count' as dataset, coalesce(b.d, s.d) as event_date, b.d is not null as in_batch, s.d is not null as in_streaming
from s_b b full outer join s_s s on b.d = s.d
union all
select 'signal', coalesce(b.d, s.d), b.d is not null, s.d is not null
from g_b b full outer join g_s s on b.d = s.d
order by 1, 2