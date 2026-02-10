with 

source_address as (
    select * 
    from {{ source('adv_works', 'production_productcategory') }}
),

renamed as (
    select  
        cast(productcategoryid as int) as product_category_id
        , cast(name as string) as category_name
        
    from source_address
)

select * from renamed