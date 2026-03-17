with fb_daily as (
    select * from {{ ref('int_facebook_ads__daily_unified') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['report_date_start', 'campaign_name', 'source_granularity']) }} as facebook_ads_daily_key,
    report_date_start,
    report_date_end,
    campaign_name,
    source_granularity,
    ad_set_budget,
    ad_set_budget_type,
    amount_spent_usd,
    campaign_delivery,
    reach,
    impressions,
    frequency,
    website_purchases,
    website_purchases_conversion_value,
    website_purchase_roas,
    link_clicks,
    cost_per_purchase_usd,
    cpm_usd
from fb_daily
