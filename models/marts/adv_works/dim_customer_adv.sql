with base as (

    select *
    from {{ ref('int_person__customer') }}

)

select 

    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk

    , base.customer_id
    , base.full_name
    , base.person_id
    , base.address
    , base.city
    , base.province_id
    , base.postal_code
    , base.country_region_code
    , base.province_name
    , base.territory_id
    , base.country_name
    

from base