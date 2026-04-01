with 

source_address as (
    select * 
    from {{ source('adv_works', 'person_person') }}
),

renamed as (
    select  
        cast(businessentityid as int) as business_entity_id
        , cast (persontype as string) as person_type
        , cast (firstname as string) as first_name
        , cast (lastname as string) as last_name
    from source_address
)

select * from renamed