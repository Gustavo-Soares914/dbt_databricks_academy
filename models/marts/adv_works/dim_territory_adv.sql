with base as (

    select *
    from {{ ref('stg_sales__salesterritory') }}
)

select 

    {{ dbt_utils.generate_surrogate_key(['territory_id']) }} as territory_sk
    , territory_id
    , territory_name
    , country_region_code
    , group as territory_group
    --, sales_ytd, vou colocar essas metricas apenas na fato vendas para evitar uma modelagem hibrida e quebrar caso tenham novas vendas
    --, sales_last_year

from base  