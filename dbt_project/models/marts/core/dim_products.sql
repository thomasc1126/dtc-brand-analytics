with product_mapping as (
    select * from {{ ref('product_mapping') }}
),

deduplicated as (
    select
        anonymized_product_code,
        min(anonymized_base_product_id) as anonymized_base_product_id,
        min(anonymized_variant_id) as anonymized_variant_id,
        min(anonymized_sku) as anonymized_sku,
        min(anonymized_product_name) as anonymized_product_name,
        min(product_family) as product_family,
        min(product_form) as product_form,
        min(pack_size_months) as pack_size_months,
        min(is_bundle) as is_bundle,
        min(active) as active
    from product_mapping
    group by anonymized_product_code
)

select * from deduplicated
