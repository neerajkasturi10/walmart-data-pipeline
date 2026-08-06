select 
    product_id,
    prod_name as product_name,
    prod_cat as product_category,
    prod_brand as product_brand,
    prod_price as product_price,
    case 
        when prod_price > 150 then 'Premium' 
        when prod_price > 100 and prod_price <= 150 then 'Mid-Range' 
        when prod_price > 0 and prod_price <= 100 then 'Budget' 
        else 'Unknown' 
    end as product_price_tier,
    prod_created_ts as product_created_timestamp,
    prod_updated_ts as product_updated_timestamp,
    prod_is_active as product_is_active
from {{ref('obt_b')}}