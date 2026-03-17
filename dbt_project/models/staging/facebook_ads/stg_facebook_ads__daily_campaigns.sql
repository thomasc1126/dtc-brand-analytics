with src as (
    select * from {{ source('facebook_ads', 'raw_facebook_ads_daily_campaigns') }}
)

select
    cast(reporting_starts as date) as report_date_start,
    cast(reporting_ends as date) as report_date_end,
    cast(campaign_name as string) as campaign_name,
    cast(ad_set_budget as float64) as ad_set_budget,
    cast(ad_set_budget_type as string) as ad_set_budget_type,
    cast(amount_spent_usd as float64) as amount_spent_usd,
    cast(campaign_delivery as string) as campaign_delivery,
    cast(reach as int64) as reach,
    cast(impressions as int64) as impressions,
    cast(frequency as float64) as frequency,
    cast(website_purchases as int64) as website_purchases,
    cast(website_purchases_conversion_value as float64) as website_purchases_conversion_value,
    cast(website_purchase_roas as float64) as website_purchase_roas,
    cast(link_clicks as int64) as link_clicks,
    cast(cost_per_purchase_usd as float64) as cost_per_purchase_usd,
    cast(cpm_usd as float64) as cpm_usd

from src
