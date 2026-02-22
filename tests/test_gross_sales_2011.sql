select
    round(sum(gross_amount), 2) as gross_2011
from {{ ref('fact_sales_adv') }}
where order_date_sk between 20110101 and 20111231
having round(sum(gross_amount), 2) != 12646112.16