{{ config(materialized='table', schema='marts') }}

select
  id,
  customer_name_upper,
  region,
  concat(region, '-', id) as customer_key
from {{ ref('staging_model') }}
order by id
