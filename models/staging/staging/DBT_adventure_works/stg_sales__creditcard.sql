with 

source_creditcard as (
    select * 
    from {{ source('adv_works', 'sales_creditcard') }}
),

renamed as (
    select  
        cast(creditcardid as bigint) as cartao_id_pk
        , cast (cardtype as string) as tipo_cartao
        , cast (cardnumber as bigint) as numero_cartao
        , cast (expmonth as int) as exp_month
        , cast ( expyear as int) as exp_year
    from source_creditcard
)

select * from renamed