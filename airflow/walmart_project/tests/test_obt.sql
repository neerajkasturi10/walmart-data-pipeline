{{ config(severity = 'warn')}}


select 1 
from {{ ref('obt_b')}} 
where  
    order_id is null
    or customer_id is null
    or order_item_id is null
    or product_id is null
    or store_id is null
    or employee_id is null
