{{ config(schema = 'STAGING', materialized = 'table')}}

SELECT
    payment_id,
    transaction_id,
    method,
    status,
    refund_flag,
    CAST(amount AS NUMBER(10,2)) AS refund_amount,      
    CAST(timestamp AS timestamp) AS timestamp
FROM {{ source('raw', 'refunds') }}