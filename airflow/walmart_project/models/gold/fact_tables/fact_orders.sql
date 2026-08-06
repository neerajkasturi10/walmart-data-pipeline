select
    order_id,
    order_item_id,
    customer_id,
    store_id,
    employee_id,
    product_id,
    oi_quantity as quantity,
    oi_unit_price as unit_price,
    oi_line_amt as line_amount,
    ord_total_amt as total_amount
from {{ ref('obt_b') }}