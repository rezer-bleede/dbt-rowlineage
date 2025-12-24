{{ config(materialized='table', schema='marts') }}

select
  region,
  count(*) as customer_count,
  array_agg(customer_name_upper order by id) as customers
from {{ ref('staging_model') }}
group by region
order by region
