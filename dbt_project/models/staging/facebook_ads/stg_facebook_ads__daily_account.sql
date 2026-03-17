with src as (
    select * from {{ source('facebook_ads', 'raw_facebook_ads_daily_account') }}
)

select
    cast(reporting_starts as date) as report_date_start,
    cast(reporting_ends as date) as report_date_end,
    cast(account_name as string) as account_name,
    cast(reach as int64) as reach,
    cast(impressions as int64) as impressions,
    cast(frequency as float64) as frequency,
    cast(amount_spent_usd as float64) as amount_spent_usd,
    cast(link_clicks as int64) as link_clicks,
    cast(website_purchases as int64) as website_purchases,
    cast(website_purchases_conversion_value as float64) as website_purchases_conversion_value,
    cast(cost_per_purchase_usd as float64) as cost_per_purchase_usd,
    cast(website_purchase_roas as float64) as website_purchase_roas

from src
