with ad_spend as (
    select * from {{ ref('stg_ad_spend__daily') }}
),

daily_revenue as (
    select * from {{ ref('int_daily_revenue') }}
),

spend_agg as (
    select
        spend_date,
        sum(spend) as total_ad_spend,
        sum(case when platform = 'facebook' then spend else 0 end) as facebook_spend,
        sum(case when platform = 'google_ads' then spend else 0 end) as google_spend,
        sum(case when platform = 'bing' then spend else 0 end) as bing_spend,
        sum(case when platform = 'pinterest' then spend else 0 end) as pinterest_spend
    from ad_spend
    group by spend_date
)

select
    {{ dbt_utils.generate_surrogate_key(['s.spend_date']) }} as daily_ad_performance_key,
    s.spend_date,
    s.total_ad_spend,
    s.facebook_spend,
    s.google_spend,
    s.bing_spend,
    s.pinterest_spend,
    dr.order_count,
    dr.revenue as shopify_revenue,
    dr.quantity,
    dr.avg_order_value,
    round(safe_divide(dr.revenue, s.total_ad_spend), 2) as blended_roas
from spend_agg as s
left join daily_revenue as dr
    on s.spend_date = dr.order_date
