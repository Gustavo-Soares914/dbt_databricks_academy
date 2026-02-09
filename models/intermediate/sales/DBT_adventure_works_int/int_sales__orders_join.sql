with

    -- import ctes

    sales_orderheader as (
        select *
        from {{ ref('stg_sales__salesorderheader') }}
    )

    , sales_order_detail as (
        select *
        from {{ ref('stg_sales__salesorderdetail') }}
    )

    -- transformation

    , joined as (
        select
            sales_order_header.salesorderid_pk as sales_order_id
            , sales_order_header.order_date
            , sales_order_header.cliente_id as customer_id
            , sales_order_header.territory_id
            , sales_order_header.creditcard_id as credit_card_id
            , sales_order_header.subtotal
            , sales_order_header.taxamt 
            , sales_order_header.freight
            , sales_order_header.total_due
            
            , sales_order_detail.salesorderdetailid_pk as sales_order_detail_id
            , sales_order_detail.salesorder_id as order_id
            , sales_order_detail.quantidade as order_qty
            , sales_order_detail.produto_id as product_id
            , sales_order_detail.specialoffer_id as special_offer_id
            , sales_order_detail.preco_unitario as unit_price
            , sales_order_detail.desconto_unitario as unit_price_discount,

            
        from sales_order_header
        inner join sales_order_detail 
        on sales_order_header.salesorderid_pk = sales_order_detail.salesorder_id
    )
select * from joined