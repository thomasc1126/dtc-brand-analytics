with orders as (
    select
        customer_id,
        cast(order_timestamp as date) as order_date
    from {{ ref('stg_shopify__orders') }}
    where customer_id is not null
),

customer_cohorts as (
    select
        customer_id,
        date_trunc(min(order_date), month) as cohort_month
    from orders
    group by customer_id
),

order_months as (
    select distinct
        o.customer_id,
        c.cohort_month,
        date_trunc(o.order_date, month) as order_month
    from orders as o
    inner join customer_cohorts as c
        on o.customer_id = c.customer_id
),

retention as (
    select
        cohort_month,
        order_month,
        date_diff(order_month, cohort_month, month) as months_since_first_order,
        count(distinct customer_id) as active_customers
    from order_months
    group by cohort_month, order_month
),

cohort_sizes as (
    select
        cohort_month,
        count(distinct customer_id) as cohort_size
    from customer_cohorts
    group by cohort_month
)

select
    {{ dbt_utils.generate_surrogate_key(['r.cohort_month', 'r.months_since_first_order']) }} as cohort_retention_key,
    r.cohort_month,
    r.months_since_first_order,
    cs.cohort_size,
    r.active_customers,
    round(safe_divide(r.active_customers, cs.cohort_size) * 100, 2) as retention_rate_pct
from retention as r
inner join cohort_sizes as cs
    on r.cohort_month = cs.cohort_month
