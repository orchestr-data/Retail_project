{{ config(schema = 'STAGING', materialized = 'table')}}

SELECT
    return_id,
    transaction_id,
    trim(reason_code) as reason_code,
    return_date,
    trim(resolution_status) as resolution_status,
    current_date as processed_date
FROM {{ source('raw', 'returns') }}