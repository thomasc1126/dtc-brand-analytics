with fb_monthly as (
    select * from {{ ref('stg_facebook_ads__monthly_adset') }}
),

google_search as (
    select * from {{ ref('stg_google_ads__search_keywords') }}
),

google_shopping as (
    select * from {{ ref('stg_google_ads__shopping_products') }}
),

fb_agg as (
    select
        'facebook' as platform,
        date_trunc(report_date, month) as report_month,
        sum(amount_spent_usd) as cost,
        sum(impressions) as impressions,
        sum(website_purchases) as conversions,
        sum(website_purchases_conversion_value) as conversion_value
    from fb_monthly
    group by 1, 2
),

google_search_agg as (
    select
        'google_ads_search' as platform,
        report_month,
        sum(cost) as cost,
        sum(impressions) as impressions,
        sum(conversions) as conversions,
        sum(conversion_value) as conversion_value
    from google_search
    group by 1, 2
),

google_shopping_agg as (
    select
        'google_ads_shopping' as platform,
        report_month,
        sum(cost) as cost,
        sum(impressions) as impressions,
        sum(conversions) as conversions,
        sum(conversion_value) as conversion_value
    from google_shopping
    group by 1, 2
)

select * from fb_agg
union all
select * from google_search_agg
union all
select * from google_shopping_agg
