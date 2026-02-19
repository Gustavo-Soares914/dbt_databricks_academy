{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('int_sales__orders_items_join') }}
)

, dim_customer as (
        select customer_id, customer_sk
        from {{ ref('dim_customer_adv') }}
)

, dim_product as (
        select product_id, product_sk
        from {{ ref('dim_products_adv') }}
)

, dim_territory as (
        select territory_id, territory_sk
        from {{ ref('dim_territory_adv') }}
)

, dim_credit_card as (
        select credit_card_id, credit_card_sk
        from {{ ref('dim_credit_card_adv') }}
)

, dim_date as (
        select date_day, date_sk
        from {{ ref('dim_date_adv') }}
)

select 
    base.sales_order_detail_id

    , dim_customer.customer_sk
    , dim_product.product_sk
    , dim_territory.territory_sk
    , dim_credit_card.credit_card_sk
    , dim_date.date_sk

    , base.order_qty
    , base.unit_price
    , base.unit_price_discount
    , base.gross_amount
    , base.discount_amount
    , base.net_amount

from base

left join dim_customer
    on base.customer_id = dim_customer.customer_id

left join dim_product
    on base.product_id = dim_product.product_id     

left join dim_territory
    on base.territory_id = dim_territory.territory_id

left join dim_credit_card
    on base.credit_card_id = dim_credit_card.credit_card_id

left join dim_date
    on cast(date_format(base.order_date, 'yyyyMMdd') as int) = dim_date.date_sk