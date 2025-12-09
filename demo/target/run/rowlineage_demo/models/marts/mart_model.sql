



  create  table "demo"."public_marts"."mart_model__dbt_tmp"


    as

  (


select
  id,
  customer_name_upper,
  region,
  concat(region, '-', id) as customer_key
from "demo"."public_staging"."staging_model"
order by id
  );
