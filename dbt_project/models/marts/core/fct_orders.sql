with enriched_orders as (
    select * from {{ ref('int_orders__enriched') }}
),

customer_spine as (
    select * from {{ ref('int_customer__spine') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['eo.order_id']) }} as order_key,
    eo.order_id,
    eo.order_timestamp,
    eo.customer_id,
    eo.customer_email_hash,
    eo.revenue,
    eo.quantity,
    eo.cost,
    eo.shipping,
    eo.tax,
    eo.discount,
    eo.amount_refunded,
    eo.order_status,
    eo.day_of_week,
    eo.device,
    eo.channel,
    eo.source,
    eo.state,
    eo.country_code,
    eo.order_source,
    eo.is_gift_card,
    eo.gift_card_revenue,
    eo.fulfillment_status,
    eo.cancelled_at,
    eo.closed_at,
    eo.cancel_reason,
    eo.line_item_count_actual,
    eo.distinct_product_family_count,
    eo.product_families,
    eo.items_total_quantity,
    eo.total_line_item_revenue,
    cs.first_order_date,
    cs.total_orders as customer_total_orders,
    cs.total_revenue as customer_total_revenue,
    cs.is_repeat_customer,
    cs.customer_lifetime_days,
    eo.order_timestamp = cs.first_order_date as is_first_order
from enriched_orders as eo
left join customer_spine as cs
    on eo.customer_id = cs.customer_id
