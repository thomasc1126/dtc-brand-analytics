with src as (
    select * from {{ source('ad_spend', 'raw_daily_ad_spend') }}
)

select
    cast(platform as string) as platform,
    cast(date as date) as spend_date,
    cast(spend as float64) as spend

from src
