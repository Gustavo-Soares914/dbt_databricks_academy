with base as (

    select *
    from {{ ref('stg_sales__salesterritory') }}
)

select 

    {{ dbt_utils.generate_surrogate_key(['territory_id']) }} as territory_sk
    , territory_id
    , territory_name
    , country_region_code
    , group
    , sales_ytd
    , sales_last_year

from base  