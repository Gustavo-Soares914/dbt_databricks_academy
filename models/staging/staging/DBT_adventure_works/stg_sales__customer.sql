with 

source_customer as (
    select * 
    from {{ source('adv_works', 'sales_customer') }}
),

renamed as (
    select  
        cast(customerid as int) as customerid_pk
        , cast (personid as int) as person_id
        , cast (territoryid as int) as territory_id
    from source_customer
)

select * from renamed
