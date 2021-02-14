{{ config(materialized='view') }}

with aggregates as(
    select MEDIAN(BALANCE) as BALANCE_MEDIAN 
    from {{ ref('customers_state_current') }}
    where BALANCE <> 0
)
select CUSTOMER_ID,
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
from {{ ref('customers_state_current') }} churn, aggregates
