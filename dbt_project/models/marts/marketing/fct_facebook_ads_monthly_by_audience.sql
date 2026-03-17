with fb_monthly as (
    select * from {{ ref('stg_facebook_ads__monthly_adset') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['audience_type', 'report_date']) }} as fb_monthly_audience_key,
    audience_type,
    date_trunc(report_date, month) as report_month,
    sum(impressions) as impressions,
    sum(amount_spent_usd) as amount_spent_usd,
    sum(website_purchases) as website_purchases,
    sum(website_purchases_conversion_value) as website_purchases_conversion_value,
    round(safe_divide(sum(website_purchases_conversion_value), nullif(sum(amount_spent_usd), 0)), 2) as platform_roas,
    round(safe_divide(sum(amount_spent_usd), nullif(sum(website_purchases), 0)), 2) as cost_per_purchase
from fb_monthly
group by 1, 2, 3
