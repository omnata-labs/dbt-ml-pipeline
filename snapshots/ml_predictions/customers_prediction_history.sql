{% snapshot customers_prediction_history %}

{{
    config(
      target_database='demo_db',
      target_schema='dbt_dev_james',
      unique_key='CUSTOMER_ID',
      strategy='check',
      check_cols=['CUSTOMER_ID','CREDITSCORE','GEOGRAPHY','GENDER','AGE','TENURE','BALANCE','NUMOFPRODUCTS','HASCRCARD','ISACTIVEMEMBER','ESTIMATEDSALARY']
    )
}}

select customers_snapshot.CUSTOMER_ID, 
        customers_snapshot.CREDITSCORE, 
        customers_snapshot.GEOGRAPHY::varchar(255) as GEOGRAPHY, 
        customers_snapshot.GENDER::varchar(255) as GENDER, 
        customers_snapshot.AGE, 
        customers_snapshot.TENURE, 
        customers_snapshot.BALANCE, 
        customers_snapshot.NUMOFPRODUCTS, 
        customers_snapshot.HASCRCARD, 
        customers_snapshot.ISACTIVEMEMBER, 
        customers_snapshot.ESTIMATEDSALARY,
        customers_predictions.RAW_PREDICTION[0]::numeric(10,5) as RAW_PREDICTION,
        customers_predictions.prediction
from {{ ref ('customers_state_current') }} customers_snapshot
join {{ ref ('customers_current_predictions') }} customers_predictions on customers_snapshot.CUSTOMER_ID = customers_predictions.CUSTOMER_ID


{% endsnapshot %}
