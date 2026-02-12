with

customer as (
    select
        customer_id
        , person_id
        , territory_id_pk
    from {{ ref('stg_sales__customer') }}
)

, persons as (
    select
        business_entity_id
        , person_type
        , first_name
        , last_name

    from {{ ref('stg_person__person') }}
)

, entity_address as (
    select
        business_entity_id_pk
        , address_id_pk
        , address_type_id_pk

    from {{ ref('stg_person__businessentityaddress') }}
)

, address as (
    select
        address_id
        , address
        , city
        , province_id
        , postal_code

    from {{ ref('stg_person__address') }}
)

, state as (
    select
        stateprovince_id
        , state_province_code
        , country_region_code
        , province_name
        , territory_id

    from {{ ref('stg_person__stateprovince') }}
)

, country as (
    select 
        country_region_code_pk
        , country_name

    from {{ ref('stg_person__countryregion') }}
)

select 
    customer.customer_id
    , customer.person_id

    , persons.business_entity_id
    , concat(persons.first_name, ' ',persons.last_name) as full_name

    
    , entity_address.address_type_id_pk

    , address.address_id
    , address.address
    , address.city
    , address.province_id
    , address.postal_code

    , state.country_region_code
    , state.province_name
    , state.territory_id

    , country.country_name

from customer
    left join persons
        on customer.person_id = persons.business_entity_id
    left join entity_address
        on persons.business_entity_id = entity_address.business_entity_id_pk
    left join address
        on entity_address.address_id_pk = address.address_id
    left join state 
        on address.province_id = state.stateprovince_id
    left join country
        on state.country_region_code = country.country_region_code_pk