{{ config(materialized='view') }}

with sex_encoded as (
    {{ dbt_ml_preprocessing.one_hot_encoder(source_table=ref('abalone_with_key'),
                                            source_column='sex',
                                            categories=['M','F','I'],
                                            include_columns=['ABALONE_ID']) }}
),
scaled_values as (
    {{ dbt_ml_preprocessing.standard_scaler(source_table=ref('abalone_with_key'),
                                            source_columns=['shell_length','diameter','height','whole_weight','shucked_weight','viscera_weight','shell_weight'],
                                            include_columns=['ABALONE_ID']) }}
)

select abalone.rings,
        scaled_values.shell_length_scaled,
        scaled_values.diameter_scaled,
        scaled_values.height_scaled,
        scaled_values.whole_weight_scaled,
        scaled_values.shucked_weight_scaled,
        scaled_values.viscera_weight_scaled,
        scaled_values.shell_weight_scaled,
        sex_encoded.sex_F::numeric(1,0) as sex_F,
        sex_encoded.sex_I::numeric(1,0) as sex_I,
        sex_encoded.sex_M::numeric(1,0) as sex_M
from {{ ref('abalone_with_key') }} as abalone
join sex_encoded on abalone.abalone_id = sex_encoded.abalone_id
join scaled_values on abalone.abalone_id = scaled_values.abalone_id
