with 

source_address as (
    select * 
    from {{ source('adv_works', 'person_address') }}
),

renamed as (
    select  
        cast(addressid as int) as endereco_id_pk
        , cast (addressline1 as string) as endereco
        , cast (city as string) as cidade
        , cast (stateprovinceid as int) as provincia
        , cast (postalcode as string) as codigo_postal
    from source_address
)

select * from renamed