with 

source_store as (
    select * 
    from {{ source('adv_works', 'sales_store') }}
),

renamed as (
    select 
        cast(businessentityid as int) as business_entity_id
        , cast(name as string) as store_name
        , cast(salespersonid as int) as person_store_id
        from source_store
)

select * from renamed