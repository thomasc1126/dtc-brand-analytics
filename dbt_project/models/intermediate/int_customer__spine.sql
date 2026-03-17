with orders as (
    select * from {{ ref('stg_shopify__orders') }}
),

customer_agg as (
    select
        customer_id,
        min(order_timestamp) as first_order_date,
        max(order_timestamp) as last_order_date,
        count(distinct order_id) as total_orders,
        sum(revenue) as total_revenue,
        sum(quantity) as total_quantity,
        round(safe_divide(sum(revenue), count(distinct order_id)), 2) as avg_order_value,
        min(state) as state,
        min(country_code) as country_code
    from orders
    group by customer_id
)

select
    customer_id,
    first_order_date,
    last_order_date,
    total_orders,
    total_revenue,
    total_quantity,
    avg_order_value,
    total_orders > 1 as is_repeat_customer,
    date_diff(cast(last_order_date as date), cast(first_order_date as date), day) as customer_lifetime_days,
    state,
    country_code
from customer_agg
