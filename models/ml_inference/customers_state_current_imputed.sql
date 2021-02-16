{{ config(
  materialized='incremental',
  unique_key='CUSTOMER_ID')
}}

with aggregates as(
    select MEDIAN(BALANCE) as BALANCE_MEDIAN 
    from {{ ref('customers_state_current') }}
    where BALANCE <> 0
)
select CUSTOMER_ID,
        DBT_UPDATED_AT,
        CREDITSCORE,
        GEOGRAPHY,
        GENDER,
        AGE,
        TENURE,
        case when BALANCE=0 then BALANCE_MEDIAN else BALANCE end AS BALANCE,
        NUMOFPRODUCTS,
        HASCRCARD,
        ISACTIVEMEMBER,
        ESTIMATEDSALARY
from {{ ref('customers_state_current') }} customer_state, aggregates

{% if is_incremental() %}

  where customer_state.DBT_UPDATED_AT > (select max(DBT_UPDATED_AT) from {{ this }})

{% endif %}
