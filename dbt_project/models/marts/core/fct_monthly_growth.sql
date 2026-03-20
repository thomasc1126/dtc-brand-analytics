with channel_overview as (
    select * from {{ ref('fct_monthly_channel_overview') }}
),

with_prior as (
    select
        report_month,
        shopify_order_count,
        shopify_revenue,
        total_ad_spend,
        blended_roas,
        blended_cac,
        new_customer_orders,
        retail_revenue,
        ga_sessions,
        lag(shopify_revenue) over (order by report_month) as prior_month_shopify_revenue,
        lag(total_ad_spend) over (order by report_month) as prior_month_ad_spend,
        lag(shopify_order_count) over (order by report_month) as prior_month_order_count,
        lag(new_customer_orders) over (order by report_month) as prior_month_new_customers,
        lag(retail_revenue) over (order by report_month) as prior_month_retail_revenue,
        lag(ga_sessions) over (order by report_month) as prior_month_ga_sessions
    from channel_overview
)

select
    {{ dbt_utils.generate_surrogate_key(['report_month']) }} as monthly_growth_key,
    report_month,
    shopify_revenue,
    prior_month_shopify_revenue,
    round(safe_divide(shopify_revenue - prior_month_shopify_revenue, prior_month_shopify_revenue) * 100, 2) as shopify_revenue_mom_pct,
    total_ad_spend,
    prior_month_ad_spend,
    round(safe_divide(total_ad_spend - prior_month_ad_spend, prior_month_ad_spend) * 100, 2) as ad_spend_mom_pct,
    shopify_order_count,
    prior_month_order_count,
    round(safe_divide(shopify_order_count - prior_month_order_count, prior_month_order_count) * 100, 2) as order_count_mom_pct,
    new_customer_orders,
    prior_month_new_customers,
    round(safe_divide(new_customer_orders - prior_month_new_customers, prior_month_new_customers) * 100, 2) as new_customers_mom_pct,
    retail_revenue,
    prior_month_retail_revenue,
    round(safe_divide(retail_revenue - prior_month_retail_revenue, prior_month_retail_revenue) * 100, 2) as retail_revenue_mom_pct,
    ga_sessions,
    prior_month_ga_sessions,
    round(safe_divide(ga_sessions - prior_month_ga_sessions, prior_month_ga_sessions) * 100, 2) as ga_sessions_mom_pct,
    blended_roas,
    blended_cac
from with_prior
