{{ config(materialized='incremental') }}
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
                                        gender_male)) as raw_prediction,
        iff(raw_prediction[0]>0.5,true,false) as prediction
from {{ ref('customers_state_current_preprocessed') }} preprocessed

{% if is_incremental() %}

  where preprocessed.DBT_UPDATED_AT > (select max(DBT_UPDATED_AT) from {{ this }})

{% endif %}

