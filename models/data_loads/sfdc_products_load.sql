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
where 1=1
