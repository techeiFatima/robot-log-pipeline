-- Success test, part 1. Returns a row (and so fails) for every raw row marked
-- as a stop that is not a 'stop' row in the clean table, and for every clean
-- 'stop' row that raw does not mark as a stop. Matched row by row.
select
    coalesce(r.source_file, c.source_file) as source_file,
    coalesce(r."Num", c.row_num)            as row_num,
    r."Robot_ProtectiveStop"                as raw_stop_value,
    c.stop_status                           as clean_stop_status
from {{ source('raw', 'raw_logs') }} r
full outer join {{ ref('stg_robot_logs') }} c
  on r.source_file = c.source_file and r."Num" = c.row_num
where (r."Robot_ProtectiveStop" = 1 and coalesce(c.stop_status, '') <> 'stop')
   or (c.stop_status = 'stop' and (r."Robot_ProtectiveStop" is null or r."Robot_ProtectiveStop" <> 1))
