{{ config(
    schema = "marts",
    materialized = "incremental",
    unique_key = "shipment_id",
    incremental_strategy='merge',
    on_schema_change='sync'
)}}

SELECT 
   shipment_id,
   order_id,
   product_id,
   from_warehouse,
   to_store,
   ship_date,
   arrival_date,
   etl_loaded_at

FROM {{ ref("stg_shipments") }}

{% if is_incremental() %}
  where etl_loaded_at > (select max(etl_loaded_at) from {{ this }})
{% endif %}