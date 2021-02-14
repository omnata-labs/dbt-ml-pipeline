{% snapshot customer_churn_snapshot %}

{{
    config(
      target_database='demo_db',
      target_schema='dbt_dev_james',
      unique_key='CUSTOMER_ID',
      strategy='check',
      check_cols='all'
    )
}}

with active_account_count as(
select CUSTOMER_ID,count(*) account_count
from {{ source('bank','customer_products') }}
where END_DATE IS NULL
group by customer_id
)
select customers.CUSTOMER_ID,
        iff(active_account_count.account_count>0,0,1) as EXITED
from {{ source('bank','customers') }}
left outer join active_account_count on active_account_count.customer_id = customers.customer_id

{% endsnapshot %}