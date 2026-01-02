{{ config(materialized='table', schema='marts') }}

select
  region,
  count(customer_count) as ccount
from {{ ref('region_rollup2') }}
group by region
order by region