-- Adds order-dependent facts to each clean row: the time since the previous
-- row, recording gaps, stop episodes and contiguous cycle stretches.
-- Rows are ordered by row_num inside each source file.

with logs as (

    select * from {{ ref('stg_robot_logs') }}

),

with_previous as (

    select
        *,
        lag(event_ts)    over (partition by source_file order by row_num) as previous_event_ts,
        lag(stop_status) over (partition by source_file order by row_num) as previous_stop_status,
        lag(cycle_id)    over (partition by source_file order by row_num) as previous_cycle_id
    from logs

),

with_flags as (

    select
        *,
        {{ dbt.datediff('previous_event_ts', 'event_ts', 'millisecond') }} / 1000.0
            as seconds_since_previous,
        coalesce(
            {{ dbt.datediff('previous_event_ts', 'event_ts', 'millisecond') }} / 1000.0
                > {{ var('gap_threshold_seconds') }},
            false
        ) as is_after_gap
    from with_previous

),

with_starts as (

    select
        *,
        -- A stop episode starts on a 'stop' row whose previous row was not a
        -- 'stop' row, or that follows a recording gap.
        case
            when stop_status = 'stop'
                 and (coalesce(previous_stop_status, '') <> 'stop' or is_after_gap)
            then 1 else 0
        end as is_stop_episode_start,
        case
            when previous_cycle_id is null or cycle_id <> previous_cycle_id then 1 else 0
        end as is_cycle_stretch_start
    from with_flags

),

numbered as (

    select
        *,
        sum(is_stop_episode_start)  over (partition by source_file order by row_num
                                          rows between unbounded preceding and current row)
            as stop_episode_seq,
        sum(is_cycle_stretch_start) over (partition by source_file order by row_num
                                          rows between unbounded preceding and current row)
            as cycle_stretch_seq
    from with_starts

)

select
    log_id,
    source_file,
    row_num,
    event_ts,
    cycle_id,
    source_file || ':cycle-stretch-' || cast(cycle_stretch_seq as {{ dbt.type_string() }})
        as cycle_stretch_id,
    stop_status,
    protective_stop,
    is_status_unknown,
    grip_lost,
    seconds_since_previous,
    is_after_gap,
    case when stop_status = 'stop'
         then source_file || ':stop-' || cast(stop_episode_seq as {{ dbt.type_string() }})
    end as stop_event_id,
    is_stop_episode_start = 1 as is_stop_event_start,
    current_j0, current_j1, current_j2, current_j3, current_j4, current_j5,
    temperature_j0, temperature_j1, temperature_j2, temperature_j3, temperature_j4, temperature_j5,
    speed_j0, speed_j1, speed_j2, speed_j3, speed_j4, speed_j5,
    tool_current
from numbered
