with 

source_address as (
    select * 
    from {{ source('adv_works', 'production_product') }}
),

renamed as (
    select  
        cast(productid as int) as product_id
        , cast(name as string) as product_name
        , cast(productnumber as string) as product_number
        , cast(productsubcategoryid as BIGINT) as product_subcategory_id
        , cast(productmodelid as BIGINT) as product_model_id
        
    from source_address
)

select * from renamed