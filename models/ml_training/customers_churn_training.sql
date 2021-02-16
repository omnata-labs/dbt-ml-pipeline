{{ config(materialized='table') }}

{% set customer_state_timestamp = "dateadd(months,-6,current_timestamp())::timestamp_tz" %}
{% set customer_churn_timestamp = "current_timestamp()::timestamp_tz" %}

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
        churn_outcome.EXITED
from {{ ref('customers_state_snapshot') }} customer_state
join {{ ref('customers_state_snapshot') }} churn_outcome 
    on customer_state.CUSTOMER_ID=churn_outcome.CUSTOMER_ID
    and {{ customer_state_timestamp }} between customer_state.DBT_VALID_FROM and coalesce(customer_state.DBT_VALID_TO,current_timestamp())
    and {{ customer_churn_timestamp }} between churn_outcome.DBT_VALID_FROM and coalesce(churn_outcome.DBT_VALID_TO,current_timestamp())


