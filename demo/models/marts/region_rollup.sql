{{ config(materialized='table', schema='marts') }}

select
  region,
  count(*) as customer_count,
  {{ ordered_array_agg('customer_name_upper', 'id') }} as customers
from {{ ref('staging_model') }}
group by region
order by region
