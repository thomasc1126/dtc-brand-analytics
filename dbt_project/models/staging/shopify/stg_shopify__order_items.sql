with src as (
    select * from {{ source('shopify', 'raw_shopify_order_items') }}
)

select
    cast(timestamp as timestamp) as order_timestamp,
    cast(order_id as string) as order_id,
    cast(transaction_id as string) as transaction_id,
    cast(order_source as string) as order_source,
    cast(status as string) as order_status,
    cast(fulfillment_status as string) as fulfillment_status,
    cast(customer_email as string) as customer_email_hash,
    cast(base_product_id as string) as base_product_id,
    cast(variant_id as string) as variant_id,
    cast(sku as string) as sku,
    cast(product as string) as product_name,
    cast(quantity as int64) as quantity,
    cast(revenue as float64) as revenue,
    cast(pre_tax_price as float64) as pre_tax_price,
    cast(tax as float64) as tax,
    cast(shipping as float64) as shipping,
    cast(discount as float64) as discount,
    cast(amount_refunded as float64) as amount_refunded,
    cast(quantity_refunded as int64) as quantity_refunded,
    cast(normalized_base_product_id as string) as normalized_base_product_id,
    cast(normalized_variant_id as string) as normalized_variant_id,
    cast(normalized_sku as string) as normalized_sku,
    cast(normalized_product_name as string) as normalized_product_name,
    cast(product_family as string) as product_family,
    cast(product_form as string) as product_form,
    cast(pack_size_months as int64) as pack_size_months,
    cast(is_bundle as boolean) as is_bundle,
    cast(product_mapping_status as string) as product_mapping_status,
    cast(mapping_key_used as string) as mapping_key_used,
    cast(mapping_notes as string) as mapping_notes

from src
