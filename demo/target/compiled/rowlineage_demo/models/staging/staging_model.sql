

select
  md5(random()::text || clock_timestamp()::text)::uuid as _row_trace_id,
  id,
  customer_name,
  region,
  upper(customer_name) as customer_name_upper
from "demo"."public_staging"."example_source"
order by id