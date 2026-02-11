with
   -- import models

   customer as (
        select 
            customer_id
            , person_id
            , territory_id
        from {{ ref('stg_sales__customer') }}
   )

, person as (
        select
            
            ,
        from {{ ref('stg_production__productcategory') }}
        select