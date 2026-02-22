with base as (

    select distinct
        sales_order_id
        , sales_reason_id
    from {{ ref('int_sales__orders_join') }}
    where sales_reason_id is not null

)

select

    {{ dbt_utils.generate_surrogate_key([
        'sales_order_id'
        , 'sales_reason_id'
    ]) }} as order_reason_sk

    , sales_order_id
    , sales_reason_id

from base
where sales_reason_id is not null