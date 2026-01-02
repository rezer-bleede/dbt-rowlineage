{{ config(materialized='table', schema='staging') }}

select
  id,
  city_name,
  city_region
from {{ ref('cities') }}
order by id
