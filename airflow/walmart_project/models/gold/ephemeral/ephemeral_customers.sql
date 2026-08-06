select
    customer_id,
    cust_f_name as customer_first_name,
    cust_l_name as customer_last_name,
    concat(cust_f_name,' ',cust_l_name) as customer_full_name,
    cust_email as customer_email,
    cust_phone as customer_phone,
    cust_city as customer_city,
    cust_province as customer_province,
    cust_country as customer_country,
    cust_created_ts as customer_created_timestamp,
    cust_updated_ts as customer_updated_timestamp,
    cust_is_active as customer_is_active,
    cust_processed_ts as customer_processed_timestamp
from {{ref('obt_b')}}