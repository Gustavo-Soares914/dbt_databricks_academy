with
   -- import models

   products as (
        select 
            product_id as product_id
            , product_name
            , product_number
            , product_subcategory_id
            , product_model_id
        from {{ ref('stg_production__product') }}
   )

, categoryes as (
        select
            product_category_id
            , category_name
        from {{ ref('stg_production__productcategory') }}
)

, model as (
        select 
            model_id
            , model_name
        from {{ ref('stg_production__productmodel') }}
)

, subcategory as (
        select 
            subcategory_id as subcategory_id
            , category_id
            , subcategory_name
        from {{ ref('stg_production__productsubcategory') }}
)

   select 
        products.product_id
        , products.product_name
        , products.product_number
        , products.product_subcategory_id -- tem como colocar uma escrita ao inves de NULL? 
        , products.product_model_id

        , categoryes.product_category_id
        , categoryes.category_name

        , model.model_id
        , model.model_name

        , subcategory.subcategory_id
        , subcategory.category_id
        , subcategory.subcategory_name

   from products
   left join subcategory
        on products.product_subcategory_id = subcategory.subcategory_id
   left join categoryes
        on subcategory.category_id = categoryes.product_category_id
   left join model
        on  products.product_model_id = model.model_id

