{{ config(
  materialized='incremental',
  unique_key='CUSTOMER_ID')
}}

{% set customer_state_timestamp = "current_timestamp()::timestamp_tz" %}

select customer_state.CUSTOMER_ID, 
        customer_state.CREDITSCORE, 
        customer_state.GEOGRAPHY, 
        customer_state.GENDER, 
        customer_state.AGE, 
        customer_state.TENURE, 
        customer_state.BALANCE, 
        customer_state.NUMOFPRODUCTS, 
        customer_state.HASCRCARD, 
        customer_state.ISACTIVEMEMBER, 
        customer_state.ESTIMATEDSALARY,
        customer_state.DBT_UPDATED_AT
from {{ ref('customers_state_snapshot') }} customer_state
where {{ customer_state_timestamp }} between customer_state.DBT_VALID_FROM and coalesce(customer_state.DBT_VALID_TO,current_timestamp())
and EXITED=0

{% if is_incremental() %}

  and customer_state.DBT_UPDATED_AT > (select coalesce(max(DBT_UPDATED_AT),'1900-01-01'::date) from {{ this }})

{% endif %}

