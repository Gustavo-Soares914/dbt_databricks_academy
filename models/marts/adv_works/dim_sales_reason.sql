with base as (

    select distinct
        sales_reason_id
        , sales_reason_name
        , sales_reason_type
    from {{ ref('stg_sales__salesreason') }}

)

, default_row as (

    select
        -1 as sales_reason_id
        , 'Sem motivo informado' as sales_reason_name
        , 'Unknown' as sales_reason_type

)

, final as (

    select * from base
    union all
    select * from default_row

)

select

    {{ dbt_utils.generate_surrogate_key(['sales_reason_id']) }} as sales_reason_sk
    , sales_reason_id
    , sales_reason_name
    , sales_reason_type

from final