with 

source_suppliers as (
    select supplier_id, company_name, contact_name, contact_title, city, country  
    from {{ source('nwind', 'suppliers') }}
),

renamed as (
    select
        cast(supplier_id as int) as supplier_pk
        , cast(company_name as string) as company_name
        , cast(contact_name as string) as contact_name
        , cast(contact_title as string) as contact_title
        , cast(city as string) as city
        , cast(country as string) as country
    from source_suppliers
)
    select * from renamed