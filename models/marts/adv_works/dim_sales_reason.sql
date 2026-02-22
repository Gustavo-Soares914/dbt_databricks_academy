with base as (

    select distinct
        sales_reason_id
        , sales_reason_name
        , sales_reason_type
    from {{ ref('stg_sales__salesreason') }}

)

select

    {{ dbt_utils.generate_surrogate_key(['sales_reason_id']) }} as sales_reason_sk
    , sales_reason_id
    , sales_reason_name
    , sales_reason_type

from base