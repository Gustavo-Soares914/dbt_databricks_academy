with 

source_categories as (
    select * from {{ source('nwind', 'categories') }}
),

renamed as (
    select
        cast(category_id as int) as category_pk
        , cast(category_name as string) as category_name
        , cast(description as string) as description
    from source_categories
)

select * from renamed