with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2021-01-01' as date)"
    ) }}
),

dates as (
    select
        cast(date_day as date) as date_day
    from date_spine
)

select
    date_day,
    extract(dayofweek from date_day) as day_of_week_num,
    format_date('%A', date_day) as day_of_week_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend,
    extract(month from date_day) as month_num,
    format_date('%B', date_day) as month_name,
    extract(quarter from date_day) as quarter,
    extract(year from date_day) as year,
    date_trunc(date_day, month) as month_start,
    date_trunc(date_day, quarter) as quarter_start,
    concat(cast(extract(year from date_day) as string), '-Q', cast(extract(quarter from date_day) as string)) as year_quarter
from dates
