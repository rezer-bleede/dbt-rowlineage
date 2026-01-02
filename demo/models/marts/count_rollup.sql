{{ config(materialized='table', schema='marts') }}

select
  count(*) as customer_count
from {{ ref('staging_model') }}