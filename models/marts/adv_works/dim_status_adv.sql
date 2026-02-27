with status as (

    select 1 as status_id, 'In Process' as status_name union all
    select 2, 'Approved' union all
    select 3, 'Backordered' union all
    select 4, 'Rejected' union all
    select 5, 'Shipped' union all
    select 6, 'Cancelled'

)

select *
from status