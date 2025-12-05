with

    -- import ctes

    products as (
        select *
        from {{ ref('stg_nwind__products') }}
    )

    , suppliers as (
        select *
        from {{ ref('stg_nwind__suppliers') }}
    )

    , categories as (
            select *
            from {{ ref('stg_nwind__categories') }}        
        )

    -- transformation

    , joined as (
        select
            products.product_pk
            , products.product_name
            , products.supplier_fk
            , products.category_fk
            , products.unit_price
            , products.units_in_stock
            , products.units_on_order
            , products.discontinued

            , suppliers.supplier_pk
            , suppliers.company_name
            , suppliers.contact_name
            , suppliers.contact_title
            , suppliers.city
            , suppliers.country

            , categories.category_pk
            , categories.category_name
            , categories.description

        from products
        inner join suppliers on products.supplier_fk = suppliers.supplier_pk
        inner join categories on products.category_fk = categories.category_pk     
    )
select * from joined