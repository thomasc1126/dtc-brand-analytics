with customer_spine as (
    select * from {{ ref('int_customer__spine') }}
)

select
    customer_id,
    first_order_date,
    last_order_date,
    total_orders,
    total_revenue,
    total_quantity,
    avg_order_value,
    is_repeat_customer,
    customer_lifetime_days,
    date_trunc(cast(first_order_date as date), month) as cohort_month,
    state,
    country_code
from customer_spine
