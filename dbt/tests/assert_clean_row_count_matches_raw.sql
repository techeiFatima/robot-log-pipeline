-- No row may be dropped or added between raw and clean.
select raw_rows, clean_rows
from (select count(*) as raw_rows from {{ source('raw', 'raw_logs') }}) r
cross join (select count(*) as clean_rows from {{ ref('stg_robot_logs') }}) c
where raw_rows <> clean_rows
