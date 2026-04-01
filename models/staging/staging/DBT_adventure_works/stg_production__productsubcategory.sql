with 

source_address as (
    select * 
    from {{ source('adv_works', 'production_productsubcategory') }}
),

renamed as (
    select  
        cast(productsubcategoryid as int) as subcategory_id
        , cast(productcategoryid as int) as category_id
        , cast(name as string) as subcategory_name
        
    from source_address
)

select * from renamed