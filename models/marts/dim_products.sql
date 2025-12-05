with
   -- import models

    int_products as (
        select *
        from {{ ref('int_products__join') }}
        )

select *
from int_products