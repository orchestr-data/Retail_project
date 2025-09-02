{{ config(
    schema = "marts",
    materialized = "incremental",
    unique_key = "payment_id"
)}}

