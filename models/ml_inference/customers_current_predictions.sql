{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID'
  ) 
}}
with candidates as(
select CUSTOMER_ID,
        DBT_UPDATED_AT,
        SAGEMAKER_INVOKE(ARRAY_CONSTRUCT(creditscore_scaled,
                                        age_scaled,
                                        tenure_scaled,
                                        balance_scaled,
                                        numofproducts_scaled,
                                        estimatedsalary_scaled,
                                        hascrcard,isactivemember,
                                        geog_france,
                                        geog_spain,
                                        geog_germany,
                                        gender_female,
                                        gender_male))[0]::numeric(10,5) as raw_prediction,
        iff(raw_prediction>0.5,true,false) as prediction
from {{ ref('customers_state_current_preprocessed') }} preprocessed

{% if is_incremental() %}
-- first part of the clause reduces the SageMaker calls
  where preprocessed.DBT_UPDATED_AT > (select coalesce(max(DBT_UPDATED_AT),'1900-01-01'::date) from {{ this }})
{% endif %}
)
select candidates.* from candidates
  left outer join {{ this }} previous 
    on candidates.CUSTOMER_ID = previous.CUSTOMER_ID
    and candidates.raw_prediction = previous.raw_prediction
    -- second part eliminates those predictions that come back matching
  where previous.CUSTOMER_ID is null
