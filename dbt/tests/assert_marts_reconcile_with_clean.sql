-- The summaries must add up to the clean table: every stop row is in exactly
-- one stop event, and the hourly and per-cycle totals match the clean counts.
with clean as (
    select
        count(*)                                                  as total_rows,
        sum(case when stop_status = 'stop' then 1 else 0 end)     as stop_rows,
        sum(case when stop_status = 'unknown' then 1 else 0 end)  as unknown_rows
    from {{ ref('stg_robot_logs') }}
),
events as (select sum(stop_rows) as stop_rows, count(*) as n from {{ ref('fct_stop_events') }}),
hourly as (
    select sum(rows_total) as total_rows, sum(stop_rows) as stop_rows,
           sum(rows_unknown_status) as unknown_rows, sum(stop_events) as stop_events
    from {{ ref('agg_stops_by_hour') }}
),
cycles as (
    select sum(rows_total) as total_rows, sum(stop_rows) as stop_rows,
           sum(rows_unknown_status) as unknown_rows, sum(stop_events) as stop_events
    from {{ ref('agg_stops_by_cycle') }}
)
select
    clean.total_rows   as clean_total_rows,
    clean.stop_rows    as clean_stop_rows,
    clean.unknown_rows as clean_unknown_rows,
    events.stop_rows   as event_stop_rows,
    events.n           as stop_events,
    hourly.total_rows  as hourly_total_rows,
    hourly.stop_rows   as hourly_stop_rows,
    hourly.stop_events as hourly_stop_events,
    cycles.total_rows  as cycle_total_rows,
    cycles.stop_rows   as cycle_stop_rows,
    cycles.stop_events as cycle_stop_events
from clean
cross join events
cross join hourly
cross join cycles
where coalesce(events.stop_rows, 0) <> clean.stop_rows
   or hourly.total_rows <> clean.total_rows
   or hourly.stop_rows <> clean.stop_rows
   or hourly.unknown_rows <> clean.unknown_rows
   or hourly.stop_events <> events.n
   or cycles.total_rows <> clean.total_rows
   or cycles.stop_rows <> clean.stop_rows
   or cycles.unknown_rows <> clean.unknown_rows
   or cycles.stop_events <> events.n
