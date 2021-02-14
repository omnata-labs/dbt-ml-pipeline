{{ config(materialized='view') }}

with geography_encoded as (
    {{ dbt_ml_preprocessing.one_hot_encoder(source_table=ref('customers_churn_training_imputed'),
                                            source_column='GEOGRAPHY',
                                            categories=['France','Spain','Germany'],
                                            include_columns=['CUSTOMER_ID']) }}
),
gender_encoded as (
    {{ dbt_ml_preprocessing.one_hot_encoder(source_table=ref('customers_churn_training_imputed'),
                                            source_column='GENDER',
                                            categories=['Female','Male'],
                                            include_columns=['CUSTOMER_ID']) }}
),
scaled_values as (
    {{ dbt_ml_preprocessing.standard_scaler(source_table=ref('customers_churn_training_imputed'),
                                            source_columns=['CREDITSCORE','AGE','TENURE','BALANCE','NUMOFPRODUCTS','ESTIMATEDSALARY'],
                                            include_columns=['CUSTOMER_ID']) }}
)
select churn.CUSTOMER_ID,
        churn.EXITED,
        scaled_values.CREDITSCORE_scaled,
        scaled_values.AGE_scaled,
        scaled_values.TENURE_scaled,
        scaled_values.BALANCE_scaled,
        scaled_values.NUMOFPRODUCTS_scaled,
        scaled_values.ESTIMATEDSALARY_scaled,
        churn.HASCRCARD,
        churn.ISACTIVEMEMBER,
        geography_encoded.GEOGRAPHY_FRANCE::numeric(1,0) as GEOG_FRANCE,
        geography_encoded.GEOGRAPHY_SPAIN::numeric(1,0) as GEOG_SPAIN,
        geography_encoded.GEOGRAPHY_GERMANY::numeric(1,0) as GEOG_GERMANY,
        gender_encoded.GENDER_Female::numeric(1,0) as GENDER_FEMALE,
        gender_encoded.GENDER_Male::numeric(1,0) as GENDER_MALE
from {{ ref('customers_churn_training_imputed') }} as churn
join geography_encoded on churn.CUSTOMER_ID = geography_encoded.CUSTOMER_ID
join gender_encoded on churn.CUSTOMER_ID = gender_encoded.CUSTOMER_ID
join scaled_values on churn.CUSTOMER_ID = scaled_values.CUSTOMER_ID
