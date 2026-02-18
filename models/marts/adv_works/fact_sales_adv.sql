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

select *
from base