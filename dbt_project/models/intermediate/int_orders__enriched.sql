with orders as (
    select * from {{ ref('stg_shopify__orders') }}
),

order_items as (
    select * from {{ ref('stg_shopify__order_items') }}
),

order_items_agg as (
    select
        order_id,
        count(*) as line_item_count_actual,
        count(distinct product_family) as distinct_product_family_count,
        string_agg(distinct product_family, ', ' order by product_family) as product_families,
        sum(quantity) as total_quantity,
        sum(revenue) as total_line_item_revenue
    from order_items
    group by order_id
)

select
    o.*,
    oia.line_item_count_actual,
    oia.distinct_product_family_count,
    oia.product_families,
    oia.total_quantity as items_total_quantity,
    oia.total_line_item_revenue
from orders as o
left join order_items_agg as oia
    on o.order_id = oia.order_id
