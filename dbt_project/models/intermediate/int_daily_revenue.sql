with orders as (
    select * from {{ ref('stg_shopify__orders') }}
)

select
    cast(order_timestamp as date) as order_date,
    count(distinct order_id) as order_count,
    sum(revenue) as revenue,
    sum(quantity) as quantity,
    round(safe_divide(sum(revenue), count(distinct order_id)), 2) as avg_order_value
from orders
group by 1
