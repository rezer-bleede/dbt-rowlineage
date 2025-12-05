{{ config(materialized='table', schema='staging') }}

select
  id,
  customer_name,
  region,
  upper(customer_name) as customer_name_upper
from {{ ref('example_source') }}
order by id
