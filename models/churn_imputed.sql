{{ config(materialized='view') }}

with aggregates as(
    select MEDIAN(BALANCE) as BALANCE_MEDIAN 
    from {{ ref('churn') }}
    where BALANCE <> 0
)
select ROWNUMBER as ID,
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

from {{ ref('churn') }} churn, aggregates
