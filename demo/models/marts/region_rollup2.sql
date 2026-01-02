{{ config(materialized='table', schema='marts') }}

select
  region,
  count(*) as customer_count
from {{ ref('staging_model') }}
group by region
order by region
