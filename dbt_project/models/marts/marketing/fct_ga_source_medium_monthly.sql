with ga as (
    select * from {{ ref('stg_ga__source_medium_monthly') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['report_month', 'source_medium']) }} as ga_source_medium_key,
    source_medium,
    report_month,
    users,
    sessions,
    revenue,
    transactions,
    avg_order_value,
    ecommerce_conversion_rate,
    per_session_value
from ga
