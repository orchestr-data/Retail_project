{{ config(
    schema = "marts",
    materialized = "table"
)}}

SELECT 
   store_id,
   location,
   region,
   store_manager,
   open_date,
   store_type,
   CURRENT_TIMESTAMP() AS etl_loaded_at

FROM {{ ref("stg_stores") }}