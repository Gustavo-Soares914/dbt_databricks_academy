with 

source_address as (
    select * 
    from {{ source('adv_works', 'person_countryregion') }}
),

renamed as (
    select  
        cast(countryregioncode as string) as country_region_code_pk
        , cast (name as string) as country_name

    from source_address
)

select * from renamed