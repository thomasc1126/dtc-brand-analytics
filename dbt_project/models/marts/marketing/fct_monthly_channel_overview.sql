with monthly_revenue as (
    select * from {{ ref('int_monthly_revenue') }}
),

monthly_ad_performance as (
    select * from {{ ref('int_monthly_ad_performance') }}
),

ga_monthly as (
    select
        report_month,
        sum(users) as ga_users,
        sum(sessions) as ga_sessions,
        sum(revenue) as ga_revenue,
        sum(transactions) as ga_transactions
    from {{ ref('stg_ga__source_medium_monthly') }}
    group by report_month
),

retail_monthly as (
    select
        date_trunc(week_ending_date, month) as report_month,
        sum(total_sales_ty_dollars) as retail_revenue,
        sum(total_sales_ty_units) as retail_units
    from {{ ref('stg_retail__weekly_sales') }}
    group by 1
),

ad_spend_monthly as (
    select
        report_month,
        sum(cost) as total_ad_spend,
        sum(case when platform = 'facebook' then cost else 0 end) as facebook_spend,
        sum(case when platform = 'google_ads_search' then cost else 0 end) as google_search_spend,
        sum(case when platform = 'google_ads_shopping' then cost else 0 end) as google_shopping_spend,
        sum(impressions) as total_impressions,
        sum(conversions) as total_platform_conversions,
        sum(conversion_value) as total_platform_conversion_value
    from monthly_ad_performance
    group by report_month
),

customer_orders as (
    select
        date_trunc(cast(order_timestamp as date), month) as order_month,
        count(distinct case
            when o.order_timestamp = cs.first_order_date then o.customer_id
        end) as new_customer_orders
    from {{ ref('stg_shopify__orders') }} as o
    inner join {{ ref('int_customer__spine') }} as cs
        on o.customer_id = cs.customer_id
    where o.customer_id is not null
    group by 1
),

combined as (
    select
        mr.order_month as report_month,
        mr.order_count as shopify_order_count,
        mr.revenue as shopify_revenue,
        mr.quantity as shopify_quantity,
        mr.avg_order_value as shopify_aov,
        asp.total_ad_spend,
        asp.facebook_spend,
        asp.google_search_spend,
        asp.google_shopping_spend,
        asp.total_impressions,
        asp.total_platform_conversions,
        asp.total_platform_conversion_value,
        gm.ga_users,
        gm.ga_sessions,
        gm.ga_revenue,
        gm.ga_transactions,
        rm.retail_revenue,
        rm.retail_units,
        co.new_customer_orders,
        round(safe_divide(mr.revenue, asp.total_ad_spend), 2) as blended_roas,
        round(safe_divide(asp.total_ad_spend, co.new_customer_orders), 2) as blended_cac,
        round(safe_divide(mr.revenue, mr.revenue + coalesce(rm.retail_revenue, 0)) * 100, 2) as shopify_revenue_mix_pct,
        round(safe_divide(coalesce(rm.retail_revenue, 0), mr.revenue + coalesce(rm.retail_revenue, 0)) * 100, 2) as retail_revenue_mix_pct
    from monthly_revenue as mr
    left join ad_spend_monthly as asp
        on mr.order_month = asp.report_month
    left join ga_monthly as gm
        on mr.order_month = gm.report_month
    left join retail_monthly as rm
        on mr.order_month = rm.report_month
    left join customer_orders as co
        on mr.order_month = co.order_month
)

select
    {{ dbt_utils.generate_surrogate_key(['report_month']) }} as monthly_channel_overview_key,
    *
from combined
