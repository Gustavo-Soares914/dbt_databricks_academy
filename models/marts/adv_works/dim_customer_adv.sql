with base as (

    select *
    from {{ ref('int_person__customer') }}

)

select 
 -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk

 -- Natural Key
    , base.customer_id

    -- Atributos
    , base.full_name
    , base.person_id

    , base.business_entity_id

    , base.address_type_id_pk

    , base.address_id
    , base.address
    , base.city
    , base.province_id
    , base.postal_code

    , base.country_region_code
    , base.province_name
    , base.territory_id

    , base.country_name
    

from base