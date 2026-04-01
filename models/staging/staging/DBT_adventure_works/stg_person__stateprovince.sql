with 

source_address as (
    select * 
    from {{ source('adv_works', 'person_stateprovince') }}
),

renamed as (
    select  
        cast(stateprovinceid as int) as stateprovince_id
        , cast(stateprovincecode as string) as state_province_code
        , cast(countryregioncode as string) as country_region_code
        , cast(name as string) as province_name
        , cast(territoryid as int) as territory_id
        
    from source_address
)

select * from renamed