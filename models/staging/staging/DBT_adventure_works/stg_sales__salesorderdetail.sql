with 

source_salesorderdetail as (
    select * 
    from {{ source('adv_works', 'sales_salesorderdetail') }}
),

renamed as (
    select  
        cast(salesorderdetailid as int) as salesorderdetailid_pk -- essa coluna não é a primeira da tabela, porem verifiquei que ela é a que possui valores unicos e sem repetição, entçao acredito que ela seja a chave primaria dessa tabla. VERIFICAR.
        , cast (salesorderid as int) as salesorder_id
        , cast (orderqty as int) as quantidade
        , cast (productid as int) as produto_id
        , cast (specialofferid as int) as specialoffer_id
        , cast (unitprice as decimal (18,6)) as preco_unitario -- verificar se eu coloquei corretamente o DECIMAL
        , cast (unitpricediscount as decimal (18,6)) as desconto_unitario -- verificar essa questão do desconto, se está em porcentagem
    
    from source_salesorderdetail
)

select * from renamed