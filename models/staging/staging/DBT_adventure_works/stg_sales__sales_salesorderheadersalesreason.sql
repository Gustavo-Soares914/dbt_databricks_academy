with 

source_salesorderheadersalesreason as (
    select * 
    from {{ source('adv_works', 'sales_salesorderheadersalesreason') }}
),

renamed as (
    select  
        cast(salesorderid as int) as salesorderid_pk
        , cast(salesreasonid as int) as sales_reason_id
    from source_salesorderheadersalesreason
)

select * from renamed