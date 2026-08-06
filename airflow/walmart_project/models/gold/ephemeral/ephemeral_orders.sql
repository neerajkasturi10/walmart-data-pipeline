select 
    order_id,
    customer_id,
    store_id as store_id,
    ord_timestamp::date as order_date,
    ord_payment_method as payment_method,
    ord_status as order_status,
    ord_total_amt as total_amount,
    ord_created_ts as order_created_timestamp,
    ord_updated_ts as order_updated_timestamp,
    ord_is_active as order_is_active
from {{ref('obt_b')}}