{%
    set configs = [
        {
            "database": "walmart",
            "schema": "silver_technical",
            "table": "orders_t",
            "columns": """
                o.order_id,
                o.order_timestamp as ord_timestamp,
                o.payment_method as ord_payment_method,
                o.order_status as ord_status,
                o.total_amount as ord_total_amt,
                o.created_timestamp as ord_created_ts,
                o.updated_timestamp as ord_updated_ts,
                o.is_active as ord_is_active,
                o.processed_timestamp as ord_processed_ts
            """,
            "alias":"o"
        },
        {
            "database": "walmart",
            "schema": "silver_technical",
            "table": "customers_t",
            "columns": """
                c.customer_id,
                c.first_name as cust_f_name,
                c.last_name as cust_l_name,
                coalesce(c.email,'Unknown') as cust_email,
                coalesce(c.phone,'Unknown') as cust_phone,
                c.city as cust_city,
                c.province as cust_province,
                c.country as cust_country,
                c.created_timestamp as cust_created_ts,
                c.updated_timestamp as cust_updated_ts,
                c.is_active as cust_is_active,
                c.processed_timestamp as cust_processed_ts
            """,
            "alias":"c",
            "join":"o.customer_id = c.customer_id"
        },
        {
            "database": "walmart",
            "schema": "silver_technical",
            "table": "order_items",
            "columns": """
                oi.order_item_id,
                oi.quantity as oi_quantity,
                oi.unit_price as oi_unit_price,
                oi.line_amount as oi_line_amt,
                oi.created_timestamp as oi_created_ts,
                oi.updated_timestamp as oi_updated_ts,
                oi.is_active as oi_is_active,
                oi.processed_timestamp as oi_processed_ts
            """,
            "alias":"oi",
            "join":"o.order_id = oi.order_id"
        },
        {
            "database": "walmart",
            "schema": "silver_technical",
            "table": "products_t",
            "columns": """
                p.product_id,
                p.product_name as prod_name,
                coalesce(p.category,'Unknown') as prod_cat,
                coalesce(p.brand,'Unknown') as prod_brand,
                coalesce(p.price,0) as prod_price,
                p.created_timestamp as prod_created_ts,
                p.updated_timestamp as prod_updated_ts,
                p.is_active as prod_is_active,
                p.processed_timestamp as prod_processed_ts
            """,
            "alias":"p",
            "join":"oi.product_id = p.product_id"
        },
        {
            "database": "walmart",
            "schema": "silver_technical",
            "table": "stores_t",
            "columns": """
                st.store_id,
                st.store_name as store_name,
                coalesce(st.city,'Unknown') as store_city,
                coalesce(st.province,'Unknown') as store_province,
                coalesce(st.country,'Unknown') as store_country,
                st.created_timestamp as store_created_ts,
                st.updated_timestamp as store_updated_ts,
                st.is_active as store_is_active,
                st.processed_timestamp as store_processed_ts
            """,
            "alias":"st",
            "join":"o.store_id = st.store_id"
        },
        {
            "database": "walmart",
            "schema": "silver_technical",
            "table": "employees_t",
            "columns": """
                e.employee_id,
                e.first_name as emp_f_name,
                e.last_name as emp_l_name,
                coalesce(e.email,'Unknown') as emp_email,
                coalesce(e.job_title,'Unknown') as emp_job_title,
                coalesce(e.salary,0) as emp_salary,
                e.created_timestamp as emp_created_ts,
                e.updated_timestamp as emp_updated_ts,
                e.is_active as emp_is_active,
                e.processed_timestamp as emp_processed_ts
            """,
            "alias":"e",
            "join":"e.store_id = st.store_id"
        }
    ]
%}

select  
    {% for config in configs %}
        {{config['columns']}}
        {% if not loop.last %}
            , 
        {%endif%}
    {% endfor %}
    , current_timestamp() as obt_processed_timestamp
from 
    {% for config in configs %}
        {% if not loop.first %}
            left join
            {{config['database']}}.{{config['schema']}}.{{config['table']}} as {{config['alias']}}
            on
            {{config['join']}}
        {%else%}
            {{config['database']}}.{{config['schema']}}.{{config['table']}} as {{config['alias']}}
        {%endif%}
    {% endfor %}


