with 

source_salesorderheader as (
    select * 
    from {{ source('adv_works', 'sales_salesorderheader') }}
),

renamed as (
    select  
        cast(salesorderid as int) as salesorderid_pk -- devo colocar PK em todas?
        , cast (orderdate as date) as order_date
        , cast (status as int) as status_id
        , cast (shipdate as date) as ship_date
        , cast (customerid as int) as cliente_id
        , cast (territoryid as int) as territory_id
        , cast (creditcardid as int) as creditcard_id
        , cast (subtotal as decimal (10,2)) as subtotal -- está correto?
        , cast (taxamt as decimal (10,2)) as taxamt -- está correto? não seri apercentual?
        , cast (freight as decimal (10,2)) as freight
        , cast (totaldue as decimal (10,2)) as total_due
    from source_salesorderheader
)

select * from renamed