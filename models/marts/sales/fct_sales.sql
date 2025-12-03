with
   -- import models

    int_sales as (
        select *
        from {{ ref('int_sales__join') }}
        )

    , metrics as (
        select
            order_item_sk
            , product_fk
            , employee_fk
            , customer_fk
            , shipper_fk
            , order_date
            , ship_date
            , required_delivery_date
            , discount_pct
            , unit_price
            , quantity
            , unit_price * quantity as gross_total
            , unit_price * (1 - discount_pct) * quantity as net_total
            , cast(
                (freight / count(*) over (partition by order_number))
            as numeric(18,2)) as freight_allocated
            , case
                when discount_pct > 0 then true
                else false
            end as had_discount
            , order_number
            , recipient_name
            , recipient_city
            , recipient_region
            , recipient_country
        from int_sales
    )

select *
from metrics