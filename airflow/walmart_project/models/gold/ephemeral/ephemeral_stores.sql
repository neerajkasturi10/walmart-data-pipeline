select 
    store_id,
    store_name,
    store_city,
    store_province,
    store_country,
    store_created_ts as store_created_timestamp,
    store_updated_ts as store_updated_timestamp,
    store_is_active
from {{ ref('obt_b') }}
