with

    -- import ctes

    sales_order_header as (
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

            (sales_order_detail.quantidade * sales_order_detail.preco_unitario) as gross_amount
            , (sales_order_detail.quantidade * sales_order_detail.preco_unitario * sales_order_detail.desconto_unitario) as discount_amount
            , (sales_order_detail.quantidade * sales_order_detail.preco_unitario * (1 - sales_order_detail.desconto_unitario)) as net_amount

        from sales_order_header
        inner join sales_order_detail 
        on sales_order_header.salesorderid_pk = sales_order_detail.salesorder_id
    )
select * from joined