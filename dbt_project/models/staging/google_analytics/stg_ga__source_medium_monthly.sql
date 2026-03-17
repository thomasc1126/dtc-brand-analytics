with src as (
    select * from {{ source('google_analytics', 'raw_ga_source_medium_monthly') }}
)

select
    cast(source_medium as string) as source_medium,
    cast(month_of_year as date) as report_month,
    cast(users as int64) as users,
    cast(sessions as int64) as sessions,
    cast(revenue as float64) as revenue,
    cast(transactions as int64) as transactions,
    cast(avg_order_value as float64) as avg_order_value,
    cast(ecommerce_conversion_rate as float64) as ecommerce_conversion_rate,
    cast(per_session_value as float64) as per_session_value

from src
