{{ config(schema = 'STAGING', materialized = 'table')}}

SELECT
    store_id,
    trim(location) as location,
    trim(region) as region,
    trim(store_manager) as store_manager,
    CAST(open_date AS DATE) AS open_date,
    trim(store_type) AS store_type
FROM {{ source('raw', 'stores') }}