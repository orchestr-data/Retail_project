{{ config(schema='STAGING',materialized='table') }}

SELECT 
   cust_id,
   ADDRESS_LINE1 as Address,
   city,
   state,
   country,
   postal_code,
   latitude,
   longitude
FROM {{ source('raw', 'customers') }}