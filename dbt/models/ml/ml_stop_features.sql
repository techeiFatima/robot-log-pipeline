-- Features and label for predicting protective stops (Phase 11).
--
-- One row per log row that is NOT already a stop and has a known status.
-- Label: does a new stop episode start within the next {{ var('ml_horizon_seconds') }} s?
-- Features use only the current row and earlier rows (no look-ahead).
-- Rows whose label window is cut by a recording gap or the end of the file
-- are kept but marked label_window_complete = false (excluded from training).

{% set h = var('ml_horizon_seconds') %}

with seq as (

    select
        *,
        {{ avg_joint_temperature() }} as avg_joint_temp,
        {{ total_abs_current() }}     as total_abs_current,
        {{ total_abs_speed() }}       as total_abs_speed
    from {{ ref('int_robot_log_sequence') }}

),

windows as (

    select
        *,
        -- look-ahead, for the LABEL only
        min(case when is_stop_event_start then event_ts end) over w_future as next_stop_start_ts,
        first_value(case when is_stop_event_start then stop_event_id end ignore nulls)
            over w_future                                                     as next_stop_event_id,
        min(case when is_after_gap then event_ts end) over w_future          as next_gap_ts,
        max(event_ts) over (partition by source_file)                        as file_last_ts,

        -- look-back FEATURES (current row and up to 9 earlier rows)
        avg(total_abs_current) over w_10       as total_abs_current_avg10,
        stddev_samp(total_abs_current) over w_10 as total_abs_current_std10,
        avg(total_abs_speed) over w_10         as total_abs_speed_avg10,
        avg(tool_current) over w_10            as tool_current_avg10,
        max(case when grip_lost then 1 else 0 end) over w_10 as grip_lost_last10,
        lag(avg_joint_temp, 10) over w_order   as avg_joint_temp_10_rows_ago,
        max(case when stop_status = 'stop' then event_ts end) over w_past as last_stop_ts,
        sum(case when is_stop_event_start then 1 else 0 end) over w_300 as stop_episodes_last300,
        row_number() over (partition by cycle_stretch_id order by row_num) as rows_into_cycle
    from seq
    window
        w_order  as (partition by source_file order by row_num),
        w_future as (partition by source_file order by row_num
                     rows between 1 following and unbounded following),
        w_10     as (partition by source_file order by row_num
                     rows between 9 preceding and current row),
        w_past   as (partition by source_file order by row_num
                     rows between unbounded preceding and 1 preceding),
        w_300    as (partition by source_file order by row_num
                     rows between 300 preceding and 1 preceding)

)

select
    log_id,
    source_file,
    row_num,
    event_ts,
    cycle_id,

    -- label (and, for evaluation only, which stop episode it points to)
    next_stop_event_id,
    coalesce(
        next_stop_start_ts <= event_ts + interval '{{ h }} seconds'
        and (next_gap_ts is null or next_gap_ts > next_stop_start_ts),
        false
    ) as stop_within_horizon,
    (next_gap_ts is null or next_gap_ts > event_ts + interval '{{ h }} seconds')
        and file_last_ts >= event_ts + interval '{{ h }} seconds'
        as label_window_complete,

    -- features: current values
    current_j0, current_j1, current_j2, current_j3, current_j4, current_j5,
    temperature_j0, temperature_j1, temperature_j2, temperature_j3, temperature_j4, temperature_j5,
    speed_j0, speed_j1, speed_j2, speed_j3, speed_j4, speed_j5,
    tool_current,
    avg_joint_temp,
    total_abs_current,
    total_abs_speed,
    case when grip_lost then 1 else 0 end as grip_lost_now,

    -- features: recent history
    total_abs_current_avg10,
    total_abs_current_std10,
    total_abs_speed_avg10,
    tool_current_avg10,
    grip_lost_last10,
    avg_joint_temp - avg_joint_temp_10_rows_ago as avg_joint_temp_change10,
    {{ dbt.datediff('last_stop_ts', 'event_ts', 'millisecond') }} / 1000.0
        as seconds_since_last_stop,
    stop_episodes_last300,
    rows_into_cycle
from windows
where stop_status = 'no_stop'
