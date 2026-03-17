with order_items as (
    select * from {{ ref('stg_shopify__order_items') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'base_product_id', 'variant_id']) }} as order_item_key,
    order_id,
    order_timestamp,
    transaction_id,
    order_source,
    order_status,
    fulfillment_status,
    customer_email_hash,
    base_product_id,
    variant_id,
    sku,
    product_name,
    quantity,
    revenue,
    pre_tax_price,
    tax,
    shipping,
    discount,
    amount_refunded,
    quantity_refunded,
    product_family,
    product_form,
    pack_size_months,
    is_bundle,
    product_mapping_status
from order_items
