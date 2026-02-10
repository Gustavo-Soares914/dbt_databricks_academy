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
        
    from source_address
)

select * from renamed