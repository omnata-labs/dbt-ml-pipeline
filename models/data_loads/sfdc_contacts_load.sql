-- depends_on: {{ ref('omnata_push','sfdc_load_tasks') }}
-- depends_on: {{ ref('omnata_push','sfdc_load_task_logs') }}
-- depends_on: {{ ref('sfdc_products_load') }}
{{
  config(
    materialized='load_task',
    operation='upsert',
    object_name='Contact',
    external_id_field='ContactNumber__c'
  )
}}

select OBJECT_CONSTRUCT('LastName',SURNAME,
                      'Birthdate',DATE_OF_BIRTH,
                      'ContactNumber__c',current_predictions.CUSTOMER_ID,
                      'Churn_Score__c',RAW_PREDICTION[0]::numeric(10,5),
                      'Churn_Flag__c',PREDICTION) as RECORD
from {{ ref('customers_current_predictions') }} current_predictions
join {{ source('bank','customers') }} customers on current_predictions.CUSTOMER_ID = customers.CUSTOMER_ID
where 1=1

{% if var('full-refresh-salesforce')==false %}
  -- this filter will only be applied on an incremental run, to prevent re-sync
  -- of previously successful, up-to-date records
  and RECORD:"ContactNumber__c"::varchar not in (
    select logs.RECORD:"ContactNumber__c"::varchar 
    from {{ ref('omnata_push','sfdc_load_task_logs') }} logs
    where logs.load_task_name= '{{ this.name }}'
    and logs.RESULT:"success" = true
    -- and logs.RESULT (timestamp?) >= DBT_UPDATED_AT
  )
{% endif %}