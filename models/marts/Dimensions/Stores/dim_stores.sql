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
   store_type

FROM {{ ref("stg_stores") }}