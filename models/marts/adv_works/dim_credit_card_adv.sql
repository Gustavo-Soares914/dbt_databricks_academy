with base as (

    select *
    from {{ ref('stg_sales__creditcard') }}
)

select 

    {{ dbt_utils.generate_surrogate_key(['cartao_id_pk']) }} as credit_card_sk
    , cartao_id_pk as credit_card_id
    , tipo_cartao as card_type
    , numero_cartao as card_number
    , exp_month
    , exp_year

from base  


