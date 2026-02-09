with 

source_salesreason as (
    select * 
    from {{ source('adv_works', 'sales_salesreason') }}
),

renamed as (
    select  
        cast(salesreasonid as int) as salesreasonid_pk -- devo colocar PK em todas?
        , cast (name as string) as nome
        , cast (reasontype as string) as motivo_da_compra
        
    from source_salesreason
)

select * from renamed