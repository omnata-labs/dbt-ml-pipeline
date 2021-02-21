-- depends_on: {{ ref('omnata_push','sfdc_load_tasks') }}
-- depends_on: {{ ref('omnata_push','sfdc_load_task_logs') }}
{{
  config(
    materialized='load_task',
    operation='upsert',
    object_name='Product2',
    external_id_field='External_Product_ID__c'
  )
}}

select OBJECT_CONSTRUCT('Name',NAME,
                      'External_Product_ID__c',PRODUCT_ID) as RECORD
from {{ source('bank','products') }} products

{% if var('full-refresh-salesforce')==false %}
  -- this filter will only be applied on an incremental run, to prevent re-sync
  -- of previously successful, up-to-date records or records that have the same score as previous
left outer join {{ ref('omnata_push','sfdc_load_task_logs') }} logs 
    on logs.RECORD:"ContactNumber__c" = current_predictions.CUSTOMER_ID 
       and logs.RESULT:"success"::boolean = true 
       and logs.load_task_name= 'sfdc_products_load'
where logs.JOB_ID is null
{% endif %}
