with customers as (
    select * from {{ ref('dim_customers') }}
),

-- Use end of data range as reference date for recency
ref_date as (
    select date('2020-12-31') as reference_date
),

rfm_scores as (
    select
        c.customer_id,
        c.cohort_month,
        c.total_orders,
        c.total_revenue,
        c.avg_order_value,
        c.is_repeat_customer,
        date_diff(rd.reference_date, cast(c.last_order_date as date), day) as days_since_last_order,
        -- RFM scoring using ntile (1 = worst, 5 = best)
        ntile(5) over (order by date_diff(rd.reference_date, cast(c.last_order_date as date), day) desc) as recency_score,
        ntile(5) over (order by c.total_orders asc) as frequency_score,
        ntile(5) over (order by c.total_revenue asc) as monetary_score
    from customers as c
    cross join ref_date as rd
    where c.customer_id is not null
),

segmented as (
    select
        *,
        recency_score + frequency_score + monetary_score as rfm_total,
        case
            when recency_score >= 4 and frequency_score >= 4 then 'champion'
            when recency_score >= 4 and frequency_score >= 2 then 'loyal'
            when recency_score >= 4 and frequency_score = 1 then 'new'
            when recency_score = 3 and frequency_score >= 3 then 'potential_loyalist'
            when recency_score = 3 and frequency_score <= 2 then 'needs_attention'
            when recency_score = 2 and frequency_score >= 3 then 'at_risk'
            when recency_score = 2 and frequency_score <= 2 then 'about_to_lose'
            when recency_score = 1 and frequency_score >= 3 then 'cant_lose_them'
            when recency_score = 1 and frequency_score <= 2 then 'lost'
            else 'other'
        end as customer_segment
    from rfm_scores
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_segment_key,
    customer_id,
    cohort_month,
    total_orders,
    total_revenue,
    avg_order_value,
    is_repeat_customer,
    days_since_last_order,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total,
    customer_segment
from segmented
