with 

source_address as (
    select * 
    from {{ source('adv_works', 'production_productmodel') }}
),

renamed as (
    select  
        cast(productmodelid as int) as model_id
        , cast(name as string) as model_name
        
    from source_address
)

select * from renamed