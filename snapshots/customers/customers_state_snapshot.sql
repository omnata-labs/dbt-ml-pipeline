{% snapshot customers_state_snapshot %}

{{
    config(
      target_database='demo_db',
      target_schema='dbt_dev_james',
      unique_key='CUSTOMER_ID',
      strategy='check',
      check_cols=['CUSTOMER_ID','CREDITSCORE','GEOGRAPHY','GENDER','AGE','TENURE','BALANCE','NUMOFPRODUCTS','HASCRCARD','ISACTIVEMEMBER','ESTIMATEDSALARY']
    )
}}

with savings_account_balance as(
select CUSTOMER_ID,SUM(AMOUNT) as BALANCE
from {{ source('bank','customer_transactions') }}
group by customer_id
),
credit_card_count as(
select CUSTOMER_ID,count(*) account_count
from {{ source('bank','customer_transactions') }}
where PRODUCT_ID in (select PRODUCT_ID from {{ source('bank','products') }} where PRODUCT_TYPE='CreditCard')
group by customer_id
),
recent_transactions_count as(
select CUSTOMER_ID,count(*) transactions_count
from {{ source('bank','customer_transactions') }}
where transaction_datetime > dateadd(day,-60,current_date())
group by customer_id
),
latest_creditscore as(
select customer_id,creditscore
from {{ source('bank','credit_check_results') }}
qualify lead(customer_id) over (partition by customer_id order by check_date) is null
),
current_tenure as(
select customer_id,start_date
from {{ source('bank','customer_products') }}
qualify lag(customer_id) over (partition by customer_id order by start_date) is null
),
active_account_count as(
select CUSTOMER_ID,count(*) account_count
from {{ source('bank','customer_products') }}
where END_DATE IS NULL
group by customer_id
)
select customers.CUSTOMER_ID,
        SURNAME,
        latest_creditscore.creditscore,
        GEOGRAPHY,
        GENDER,
        DATEDIFF(years,DATE_OF_BIRTH,CURRENT_DATE()) as AGE,
        DATEDIFF(years,current_tenure.start_date,CURRENT_DATE()) as TENURE,
        coalesce(savings_account_balance.BALANCE::number(10,2),0) as BALANCE,
        (select count(*) from CUSTOMER_PRODUCTS cp where cp.CUSTOMER_ID = customers.CUSTOMER_ID and END_DATE is null) as NUMOFPRODUCTS,
        iff(credit_card_count.account_count>0,1,0) as HASCRCARD,
        iff(recent_transactions_count.transactions_count>0,1,0) as ISACTIVEMEMBER,
        iff(active_account_count.account_count>0,0,1) as EXITED,
        ESTIMATEDSALARY
from {{ source('bank','customers') }}
left outer join savings_account_balance on savings_account_balance.customer_id = customers.customer_id
left outer join credit_card_count on credit_card_count.customer_id = customers.customer_id
left outer join recent_transactions_count on recent_transactions_count.customer_id = customers.customer_id
left outer join current_tenure on current_tenure.customer_id = customers.customer_id
left outer join latest_creditscore on latest_creditscore.customer_id = customers.customer_id
left outer join active_account_count on active_account_count.customer_id = customers.customer_id

{% endsnapshot %}
