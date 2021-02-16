{{ config(materialized='view') }}

with aggregates as(
    select MEDIAN(BALANCE) as BALANCE_MEDIAN 
    from {{ ref('customers_churn_training') }}
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
        ESTIMATEDSALARY,
        EXITED

from {{ ref('customers_churn_training') }} churn, aggregates
