{{ config(schema = 'staging', materialized='table') }}

SELECT 
    -- Extract basic fields from JSON
    src:promo_id::STRING as promotion_id,
    src:promo_type::STRING as promotion_type,
    src:product_scope::STRING as product_scope,
    src:discount_type::STRING as discount_type,
    src:start_date::DATE as start_date,
    src:end_date::DATE as end_date,
    
    -- Add a simple calculated field
    CURRENT_TIMESTAMP() as processed_at

FROM {{ source('raw', 'promotions') }}
WHERE src IS NOT NULL