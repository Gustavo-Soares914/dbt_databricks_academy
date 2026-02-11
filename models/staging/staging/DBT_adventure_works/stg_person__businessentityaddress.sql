with 

source_address as (
    select * 
    from {{ source('adv_works', 'person_businessentityaddress') }}
),

renamed as (
    select  
        cast(businessentityid as int) as business_entity_id
        , cast (addressid as int) as address_id
        , cast (addresstypeid as int) as address_type_id

    from source_address
)

select * from renamed