with daily_campaigns as (
    select * from {{ ref('stg_facebook_ads__daily_campaigns') }}
),

daily_account as (
    select * from {{ ref('stg_facebook_ads__daily_account') }}
),

campaigns_aligned as (
    select
        report_date_start,
        report_date_end,
        campaign_name,
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
        cpm_usd,
        'campaign' as source_granularity
    from daily_campaigns
),

account_aligned as (
    select
        report_date_start,
        report_date_end,
        'account_level_aggregate' as campaign_name,
        cast(null as float64) as ad_set_budget,
        cast(null as string) as ad_set_budget_type,
        amount_spent_usd,
        cast(null as string) as campaign_delivery,
        reach,
        impressions,
        frequency,
        website_purchases,
        website_purchases_conversion_value,
        website_purchase_roas,
        link_clicks,
        cost_per_purchase_usd,
        cast(null as float64) as cpm_usd,
        'account' as source_granularity
    from daily_account
)

select * from campaigns_aligned
union all
select * from account_aligned
