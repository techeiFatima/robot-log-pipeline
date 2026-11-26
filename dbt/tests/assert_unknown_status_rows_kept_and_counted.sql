-- Success test, part 2. Rows with unknown stop status must be kept and
-- labelled 'unknown', never dropped or guessed. Fails if any raw NULL row is
-- missing or relabelled, or if the counts differ. Counts come from the tables,
-- never from hardcoded numbers.
with raw_unknown as (
    select source_file, "Num" as row_num
    from {{ source('raw', 'raw_logs') }}
    where "Robot_ProtectiveStop" is null
),
clean_unknown as (
    select source_file, row_num
    from {{ ref('stg_robot_logs') }}
    where stop_status = 'unknown'
),
mismatched as (
    select coalesce(r.source_file, c.source_file) as source_file,
           coalesce(r.row_num, c.row_num)         as row_num,
           case when c.row_num is null then 'unknown in raw, not unknown in clean'
                else 'unknown in clean, not unknown in raw' end as problem
    from raw_unknown r
    full outer join clean_unknown c using (source_file, row_num)
    where r.row_num is null or c.row_num is null
)
select * from mismatched
