with src as (
    select * from {{ source('google_ads', 'raw_google_ads_shopping_products') }}
)

select
    cast(month as date) as report_month,
    cast(account as string) as account,
    cast(campaign_type as string) as campaign_type,
    cast(item_id as string) as item_id,
    cast(product_type_level1 as string) as product_type_level1,
    cast(currency as string) as currency,
    cast(cost as float64) as cost,
    cast(impressions as int64) as impressions,
    cast(ctr as float64) as ctr,
    cast(avg_cpc as float64) as avg_cpc,
    cast(conversions as float64) as conversions,
    cast(conv_rate as float64) as conversion_rate,
    cast(conv_value as float64) as conversion_value,
    cast(conv_value_per_cost as float64) as conversion_value_per_cost,
    cast(cost_per_conv as float64) as cost_per_conversion

from src
