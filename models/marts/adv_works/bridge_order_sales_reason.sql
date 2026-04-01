with orders as (

    select distinct
        sales_order_id
    from {{ ref('int_sales__orders_items_join') }}

)

, reasons as (
 
      select distinct
         sales_order_id
         , sales_reason_id
     from {{ ref('int_sales__orders_join') }}

)

, final as (
 
     select
         o.sales_order_id
         , coalesce(r.sales_reason_id, -1) as sales_reason_id
     from orders o
     left join reasons r
         on o.sales_order_id = r.sales_order_id

)

select
    {{ dbt_utils.generate_surrogate_key([
        'sales_order_id'
        , 'sales_reason_id'
    ]) }} as order_reason_sk
    , sales_order_id
    , sales_reason_id
from final