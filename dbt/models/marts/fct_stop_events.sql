-- One row per protective-stop episode: a run of consecutive log rows marked
-- as a stop. Answers "when did the robot stop itself, for how long, and
-- under what conditions?"

with seq as (

    select * from {{ ref('int_robot_log_sequence') }}

),

stop_rows as (

    select * from seq where stop_event_id is not null

),

events as (

    select
        stop_event_id,
        min(source_file)                                    as source_file,
        min(event_ts)                                       as started_at,
        max(event_ts)                                       as ended_at,
        count(*)                                            as stop_rows,
        {{ dbt.datediff('min(event_ts)', 'max(event_ts)', 'millisecond') }} / 1000.0
                                                            as duration_seconds,
        min(row_num)                                        as first_row_num,
        max(row_num)                                        as last_row_num,
        {{ count_if('grip_lost') }} > 0                     as grip_lost_during_stop,
        avg({{ avg_joint_temperature() }})                  as avg_joint_temperature,
        max({{ max_joint_temperature() }})                  as max_joint_temperature,
        avg({{ total_abs_current() }})                      as avg_total_abs_current,
        avg({{ total_abs_speed() }})                        as avg_total_abs_speed,
        avg(tool_current)                                   as avg_tool_current
    from stop_rows
    group by stop_event_id

),

onset as (

    select stop_event_id, cycle_id as cycle_id_at_start
    from stop_rows
    where is_stop_event_start

)

select
    e.stop_event_id,
    e.source_file,
    e.started_at,
    e.ended_at,
    {{ dbt.date_trunc('hour', 'e.started_at') }}            as started_hour,
    o.cycle_id_at_start,
    e.stop_rows,
    e.duration_seconds,
    e.first_row_num,
    e.last_row_num,
    e.grip_lost_during_stop,
    e.avg_joint_temperature,
    e.max_joint_temperature,
    e.avg_total_abs_current,
    e.avg_total_abs_speed,
    e.avg_tool_current,
    {{ dbt.datediff('lag(e.ended_at) over (partition by e.source_file order by e.started_at)',
                    'e.started_at', 'millisecond') }} / 1000.0
                                                            as seconds_since_previous_stop
from events e
left join onset o using (stop_event_id)
