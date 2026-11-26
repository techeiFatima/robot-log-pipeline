-- Protective stops per clock hour (UTC). The source covers one day, so
-- this is effectively "by hour of that day"; it stays correct for many days.

with seq as (

    select * from {{ ref('int_robot_log_sequence') }}

)

select
    {{ dbt.date_trunc('hour', 'event_ts') }}                as hour_utc,
    count(*)                                                as rows_total,
    {{ count_if("stop_status <> 'unknown'") }}              as rows_known_status,
    {{ count_if('is_status_unknown') }}                     as rows_unknown_status,
    {{ count_if("stop_status = 'stop'") }}                  as stop_rows,
    {{ count_if('is_stop_event_start') }}                   as stop_events,
    {{ count_if("stop_status = 'stop'") }} * 1.0
        / nullif({{ count_if("stop_status <> 'unknown'") }}, 0)
                                                            as stop_row_rate,
    {{ count_if('grip_lost') }}                             as grip_lost_rows,
    count(distinct cycle_id)                                as cycles_seen,
    min(event_ts)                                           as first_event_ts,
    max(event_ts)                                           as last_event_ts,
    sum(case when not is_after_gap then seconds_since_previous else 0 end) / 60.0
                                                            as minutes_recorded,
    avg({{ avg_joint_temperature() }})                      as avg_joint_temperature,
    avg(tool_current)                                       as avg_tool_current
from seq
group by 1
