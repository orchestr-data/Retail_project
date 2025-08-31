{{ config(schema='STAGING',materialized='table') }}

SELECT 
   CUST_ID,
   MARKETING_OPT_IN as Mark_OPT_IN,
   ACQUISITION_CHANNEL as Channel,
   LAST_LOGIN_DATE as Last_Login,
   PREFERRED_PAYMENT_METHOD as Pref_Pay_Method,
   LOYALTY_TIER
FROM {{ source('raw', 'customers') }}