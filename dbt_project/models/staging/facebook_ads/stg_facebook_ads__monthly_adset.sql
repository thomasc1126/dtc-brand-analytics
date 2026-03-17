with src as (
    select * from {{ source('facebook_ads', 'raw_facebook_ads_monthly_adset') }}
)

select
    cast(month_start_end as string) as month_start_end,
    cast(month as string) as month_label,
    cast(date as date) as report_date,
    cast(ad_set_id as string) as ad_set_id,
    cast(campaign_id as string) as campaign_id,
    cast(impressions as int64) as impressions,
    cast(amount_spent_usd as float64) as amount_spent_usd,
    cast(starts as string) as ad_set_start_date,
    cast(ends as string) as ad_set_end_date,
    cast(cpm as float64) as cpm,
    cast(website_purchases as int64) as website_purchases,
    cast(website_purchases_conversion_value as float64) as website_purchases_conversion_value,
    cast(reporting_starts as date) as reporting_starts,
    cast(reporting_ends as date) as reporting_ends,
    cast(audience_type as string) as audience_type

from src
