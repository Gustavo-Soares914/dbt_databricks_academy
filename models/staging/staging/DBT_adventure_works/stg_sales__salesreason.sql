with 

source_salesreason as (
    select * 
    from {{ source('adv_works', 'sales_salesreason') }}
),

renamed as (
    select  
        cast(salesreasonid as int) as sales_reason_id
        , cast (name as string) as sales_reason_name
        , cast (reasontype as string) as sales_reason_type
        
    from source_salesreason
)

select * from renamed