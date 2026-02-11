with

customers as (
    select
        customer_id
        , person_id
        , territory_id
    from {{ ref('stg_sales__customer') }}
)

, persons as (
    select
        business_entity_id
        , person_type
        , first_name
        , last_name

    from {{ ref( 'person_person' ) }}
)

, entity_address as (
    select
        business_entity_id
        , address_id
        , row_number() over (
            partition by businessentity_id
            order by address_id
        ) as rn
        , address_type_id

    from {{ ( 'person_businessentityaddress' ) }}
)

, address as (
    select
        address_id
        , address
        , city
        , province_id
        , postal_code

    from {{ ( 'person_address' ) }}
)

, state as (
    select
        stateprovince_id
        , state_province_code
        , country_region_code
        , province_name
        , territory_id

    from {{ ( 'person_stateprovince' ) }}
)

, country as (
    select 
        country_region_code_pk
        , country_name

    from {{ ( 'person_countryregion' ) }}

)

select 
    customer.customer_id
    , customer.person_id
    , customer.territory_id

    , persons.business_entity_id
    , persons.person_type
    , persons.concat(persons.first_name, ' ', persons.last_name) as full_name
    
    , entity_address.business_entity_id
    , entity_address.address_id
    , entity_address.rn
    , entity_address.address_type_id

    , address.address_id
    , address.address
    , address.city
    , address.province_id
    , address.postal_code

    , state.stateprovince_id
    , state.state_province_code
    , state.country_region_code
    , state.province_name
    , state.territory_id

    , country.country_region_code_pk
    , country.country_name

from customers
    left join persons
        on customer.person_id = entity_address.business_entity_id 
    left join entity_address
        on entity_address.business_entity_id = entity_address.business_entity_id
        and entity_address.rn = 1
    left join address
        on entity_address.address_id = address.address_id
    left join state 
        on address.province_id = state.stateprovince_id
    left join country
        on state.country_region_code = country.country_region_code_pk