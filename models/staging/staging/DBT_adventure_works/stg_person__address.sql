with 

source_address as (
    select * 
    from {{ source('adv_works', 'person_address') }}
),

renamed as (
    select  
        cast(addressid as int) as address_id
        , cast (addressline1 as string) as address
        , cast (city as string) as city
        , cast (stateprovinceid as int) as province_id
        , cast (postalcode as string) as postal_code
    from source_address
)

select * from renamed