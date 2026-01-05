{{ config(materialized='table', schema='marts') }}

select
  city_region,
  count(*) as customer_count
from {{ ref('staging_cities') }}
group by city_region
order by city_region
