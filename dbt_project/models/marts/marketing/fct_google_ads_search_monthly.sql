with search as (
    select * from {{ ref('stg_google_ads__search_keywords') }}
),

aggregated as (
    select
        report_month,
        keyword_category,
        sum(cost) as cost,
        sum(impressions) as impressions,
        safe_divide(sum(cost), nullif(sum(impressions), 0)) * 1000 as cpm,
        sum(conversions) as conversions,
        sum(conversion_value) as conversion_value,
        round(safe_divide(sum(conversion_value), nullif(sum(cost), 0)), 2) as conversion_value_per_cost,
        round(safe_divide(sum(cost), nullif(sum(conversions), 0)), 2) as cost_per_conversion
    from search
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['report_month', 'keyword_category']) }} as google_search_monthly_key,
    *
from aggregated
