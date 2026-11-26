-- Protective stops per robot cycle (the source's "cycle " column).
-- A cycle number can appear in more than one separate stretch of the log
-- (cycles 224-233 do), so cycle_stretches counts those stretches.

with seq as (

    select * from {{ ref('int_robot_log_sequence') }}

)

select
    cycle_id,
    count(distinct cycle_stretch_id)                        as cycle_stretches,
    min(event_ts)                                           as first_event_ts,
    max(event_ts)                                           as last_event_ts,
    count(*)                                                as rows_total,
    {{ count_if("stop_status <> 'unknown'") }}              as rows_known_status,
    {{ count_if('is_status_unknown') }}                     as rows_unknown_status,
    {{ count_if("stop_status = 'stop'") }}                  as stop_rows,
    {{ count_if('is_stop_event_start') }}                   as stop_events,
    {{ count_if("stop_status = 'stop'") }} > 0              as had_stop,
    {{ count_if('grip_lost') }}                             as grip_lost_rows,
    avg({{ avg_joint_temperature() }})                      as avg_joint_temperature,
    max({{ max_joint_temperature() }})                      as max_joint_temperature,
    avg({{ total_abs_current() }})                          as avg_total_abs_current,
    avg(tool_current)                                       as avg_tool_current
from seq
group by cycle_id
