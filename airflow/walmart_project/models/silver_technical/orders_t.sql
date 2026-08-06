{{
    config(
        materialized = 'incremental',
        database = 'walmart',
        unique_key = 'order_id',
        on_schema_change = 'append_new_column'
    )
}}

select 
    *, 
    current_timestamp() as processed_timestamp
from {{source('walmart_databricks','orders')}}


{% if is_incremental() %}
    where updated_timestamp > (select coalesce(max(updated_timestamp),'1900-01-01') from {{this}})
{% endif %}