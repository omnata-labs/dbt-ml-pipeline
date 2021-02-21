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
                      'Churn_Score__c',RAW_PREDICTION,
                      'Churn_Flag__c',PREDICTION) as RECORD
from {{ ref('customers_current_predictions') }} current_predictions
join {{ source('bank','customers') }} customers on current_predictions.CUSTOMER_ID = customers.CUSTOMER_ID

{% if var('full-refresh-salesforce')==false %}
  -- this filter will only be applied on an incremental run, to prevent re-sync
  -- of previously successful, up-to-date records or records that have the same score as previous
left outer join {{ ref('omnata_push','sfdc_load_task_logs') }} logs 
    on logs.RECORD:"ContactNumber__c" = current_predictions.CUSTOMER_ID 
       and logs.RESULT:"success"::boolean = true 
       and logs.load_task_name= 'sfdc_contacts_load'
left outer join {{ ref('omnata_push','sfdc_load_tasks') }} task 
    on logs.JOB_ID=task.JOB_ID 
       and task.CREATION_TIME >= DBT_UPDATED_AT
where task.JOB_ID is null
{% endif %}


