{{ config(materialized='table', schema='marts') }}

select
  region,
  count(*) as customer_count
from {{ ref('staging_cities') }}
group by region
order by region
