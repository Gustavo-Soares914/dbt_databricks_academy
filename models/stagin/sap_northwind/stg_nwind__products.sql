with 

source_products as (
    select * from {{ source('nwind', 'products') }}
),

renamed as (
    select
        cast(product_id as int) as product_pk
        , cast(product_name as string) as product_name
        , cast(supplier_id as int) as supplier_fk
        , cast(category_id as int) as category_fk
        , cast(unit_price as float) as unit_price
        , cast(units_in_stock as int) as units_in_stock
        , cast(units_on_order as int) as units_on_order
        , cast(discontinued as int) as discontinued
    from source_products
)
select * from renamed
