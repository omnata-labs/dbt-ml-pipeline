{{ config(materialized='view') }}

select {{ dbt_utils.surrogate_key(['sex','shell_length','diameter','height','whole_weight','shucked_weight','viscera_weight','shell_weight','rings']) }} as ABALONE_ID,*
from {{ ref('abalone') }}
