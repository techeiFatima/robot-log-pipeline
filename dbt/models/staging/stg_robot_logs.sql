-- One clean row per raw log row. Nothing is dropped and nothing is guessed:
-- rows whose stop status is missing are kept and flagged (stop_status = 'unknown').
--
-- Fixes, by profiling finding number:
--   1  "Timestamp" is text          -> event_ts, a real TIMESTAMP (UTC)
--   2  stray double quotes          -> stripped before parsing; had_stray_quotes keeps the evidence
--   5  stop stored as DOUBLE 0/1    -> protective_stop BOOLEAN + stop_status text label
--   6  "cycle " has a trailing space -> cycle_id
--   7  Temperature_T0 breaks pattern -> temperature_j0 (assumed joint 0, unconfirmed)
--   8  mixed naming styles          -> every column in snake_case
--   4  54 rows without stop status  -> is_status_unknown flag, values left NULL

with source as (

    select * from {{ source('raw', 'raw_logs') }}

),

cleaned as (

    select
        source_file || ':' || cast("Num" as {{ dbt.type_string() }}) as log_id,
        source_file,
        loaded_at,
        "Num"                                               as row_num,

        "Timestamp"                                         as timestamp_raw,
        ("Timestamp" like '"%')                             as had_stray_quotes,
        {{ parse_iso_utc_ts("replace(trim(\"Timestamp\", '\"'), 'Z', '')") }}
                                                            as event_ts,

        "Current_J0"                                        as current_j0,
        "Current_J1"                                        as current_j1,
        "Current_J2"                                        as current_j2,
        "Current_J3"                                        as current_j3,
        "Current_J4"                                        as current_j4,
        "Current_J5"                                        as current_j5,

        "Temperature_T0"                                    as temperature_j0,
        "Temperature_J1"                                    as temperature_j1,
        "Temperature_J2"                                    as temperature_j2,
        "Temperature_J3"                                    as temperature_j3,
        "Temperature_J4"                                    as temperature_j4,
        "Temperature_J5"                                    as temperature_j5,

        "Speed_J0"                                          as speed_j0,
        "Speed_J1"                                          as speed_j1,
        "Speed_J2"                                          as speed_j2,
        "Speed_J3"                                          as speed_j3,
        "Speed_J4"                                          as speed_j4,
        "Speed_J5"                                          as speed_j5,

        "Tool_current"                                      as tool_current,
        "cycle "                                            as cycle_id,

        case
            when "Robot_ProtectiveStop" = 1 then 'stop'
            when "Robot_ProtectiveStop" = 0 then 'no_stop'
            when "Robot_ProtectiveStop" is null then 'unknown'
            else 'invalid'  -- any other value fails the accepted_values test
        end                                                 as stop_status,
        case
            when "Robot_ProtectiveStop" = 1 then true
            when "Robot_ProtectiveStop" = 0 then false
        end                                                 as protective_stop,
        ("Robot_ProtectiveStop" is null)                    as is_status_unknown,

        -- Kept as delivered. In the 54 unknown-status rows it is always false,
        -- which may be a default rather than a measurement (finding 10).
        "grip_lost"                                         as grip_lost

    from source

)

select * from cleaned
