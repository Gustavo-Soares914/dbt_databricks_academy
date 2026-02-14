with 

source_territory as (
    select * 
    from {{ source('adv_works', 'sales_salesterritory') }}
),

renamed as (
    select 
        cast(territoryid as int) as territory_id
        , cast(name as string) as territory_name
        , cast(countryregioncode as string) as country_region_code
        , cast(group as string) as group
        , cast(salesytd as int) as sales_ytd
        , cast(saleslastyear as int) as sales_last_year
)

select * from renamed