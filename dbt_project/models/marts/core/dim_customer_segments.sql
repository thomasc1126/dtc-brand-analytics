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
        -- Recency: days since last order (explicit thresholds)
        case
            when date_diff(rd.reference_date, cast(c.last_order_date as date), day) <= 90 then 5
            when date_diff(rd.reference_date, cast(c.last_order_date as date), day) <= 180 then 4
            when date_diff(rd.reference_date, cast(c.last_order_date as date), day) <= 365 then 3
            when date_diff(rd.reference_date, cast(c.last_order_date as date), day) <= 545 then 2
            else 1
        end as recency_score,
        -- Frequency: order count (explicit thresholds)
        case
            when c.total_orders >= 5 then 5
            when c.total_orders = 4 then 4
            when c.total_orders = 3 then 3
            when c.total_orders = 2 then 2
            else 1  -- 1 order = lowest frequency score
        end as frequency_score,
        -- Monetary: total revenue (explicit thresholds)
        case
            when c.total_revenue >= 300 then 5
            when c.total_revenue >= 150 then 4
            when c.total_revenue >= 75 then 3
            when c.total_revenue >= 30 then 2
            else 1
        end as monetary_score
    from customers as c
    cross join ref_date as rd
    where c.customer_id is not null
),

segmented as (
    select
        *,
        recency_score + frequency_score + monetary_score as rfm_total,
        case
            when recency_score = 5 and frequency_score >= 4 then 'champions'
            when recency_score in (3, 4) and frequency_score >= 4 then 'loyal_customers'
            when recency_score in (1, 2) and frequency_score >= 4 then 'cant_lose_them'
            when recency_score in (4, 5) and frequency_score = 3 then 'potential_loyalists'
            when recency_score = 3 and frequency_score = 3 then 'need_attention'
            when recency_score in (1, 2) and frequency_score = 3 then 'at_risk'
            when recency_score = 5 and frequency_score in (1, 2) then 'new_customers'
            when recency_score = 4 and frequency_score in (1, 2) then 'promising'
            when recency_score = 3 and frequency_score in (1, 2) then 'about_to_sleep'
            when recency_score in (1, 2) and frequency_score in (1, 2) then 'hibernating'
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
