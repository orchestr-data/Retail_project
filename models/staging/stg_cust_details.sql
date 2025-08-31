{{ config(schema='STAGING',materialized='table') }}

SELECT
    cust_id,
    name,
    email,
    phone_number AS phone,
    dob,
    age,
    DATEDIFF(year, dob, signup_date) AS age_at_signup,
    gender,
    signup_date,
    customer_status AS cust_status
FROM {{ source('raw', 'customers') }}