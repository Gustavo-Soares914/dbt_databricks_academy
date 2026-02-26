with

customer as (
    select
        customer_id
        , person_id
        , territory_id_pk
        , store_id
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
    select *
    from (
        select
            business_entity_id_pk
            , address_id_pk
            , address_type_id_pk
            , row_number() over (
                partition by business_entity_id_pk
                order by address_id_pk
            ) as rn
        from {{ ref('stg_person__businessentityaddress') }}
    )
    where rn = 1
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

, store as (
    select
        business_entity_id as store_business_entity_id
        , store_name
        , person_store_id
    from {{ ref('stg_sales__store')}}
)

, person_join as (
    select 
        customer.customer_id
        , persons.business_entity_id
        , concat(persons.first_name, ' ', persons.last_name) as person_name
        , entity_address.address_id_pk
        , entity_address.address_type_id_pk

    from customer
    inner join persons
        on customer.person_id = persons.business_entity_id
    left join store
        on customer.store_id = store.store_business_entity_id
    left join entity_address
        on persons.business_entity_id = entity_address.business_entity_id_pk
)

, store_join as (
    select 
        customer.customer_id
        , store.store_business_entity_id
        , store.store_name
        , entity_address.address_id_pk
        , entity_address.address_type_id_pk

    from customer
    left join store
        on customer.store_id = store.store_business_entity_id
    left join entity_address
        on store.store_business_entity_id = entity_address.business_entity_id_pk
)

select 
    c.customer_id
    , c.person_id
    , c.store_id

    , case 
         when c.person_id is not null then 'PERSON'
         when c.store_id is not null then 'STORE'
         else 'UNKNOWN'
     end as customer_type

    , coalesce(person_join.person_name, store_join.store_name) as full_name
    , coalesce(address_p.address, address_s.address) as address
    , coalesce(address_p.city, address_s.city) as city
    , coalesce(address_p.province_id, address_s.province_id) as province_id
    , coalesce(address_p.postal_code, address_s.postal_code) as postal_code
    , coalesce(state_p.country_region_code, state_s.country_region_code) as country_region_code
    , coalesce(state_p.province_name, state_s.province_name) as province_name
    , coalesce(state_p.territory_id, state_s.territory_id) as territory_id
    , coalesce(country_p.country_name, country_s.country_name) as country_name

from customer c
    left join person_join
        on c.customer_id = person_join.customer_id
    left join address as address_p
        on person_join.address_id_pk = address_p.address_id
    left join state as state_p
        on address_p.province_id = state_p.stateprovince_id
    left join country as country_p
        on state_p.country_region_code = country_p.country_region_code_pk

    left join store_join
        on c.customer_id = store_join.customer_id

    left join address as address_s
        on store_join.address_id_pk = address_s.address_id

    left join state as state_s
        on address_s.province_id = state_s.stateprovince_id

    left join country as country_s
        on state_s.country_region_code = country_s.country_region_code_pk