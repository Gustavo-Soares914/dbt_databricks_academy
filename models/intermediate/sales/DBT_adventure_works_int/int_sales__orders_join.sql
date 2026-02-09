with

    -- import ctes

    sales_order_header as (
        select *
        from {{ ref('stg_sales__salesorderheader') }}
    )

    , sales_order_sales_reason as (
        select *
        from {{ ref('stg_sales__sales_salesorderheadersalesreason') }}
    )

       , sales_reason as (
        select *
        from {{ ref('stg_sales__salesreason') }}
    )

    -- transformation

    , joined as (
        select
            sales_order_header.salesorderid_pk as sales_order_id
            , sales_order_header.status_id as order_status
            , sales_reason.salesreasonid as sales_reason_id
            , sales_reason.name as sales_reason_name
            , sales_reason.reason_type as sales_reason_type
            

        from sales_order_header
        left join sales_order_sales_reason 
        on sales_order_header.salesorderid_pk = sales_order_sales_reason.salesorderid_pk
        left join sales_reason
        on sales_order_sales_reason.sales_reason_id = sales_reason.salesreasonid_pk
    )
    
select * from joined