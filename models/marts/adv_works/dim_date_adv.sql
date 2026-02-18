with date_spine as (

    select
        explode(
            sequence(
                to_date('2005-01-01'),
                to_date('2015-12-31'),
                interval 1 day
            )
        ) as full_date

)

select

    cast(date_format(full_date, 'yyyyMMdd') as int) as date_sk
    , full_date as date_day
    , year(full_date) as year
    , month(full_date) as month
    , day(full_date) as day
    , date_format(full_date, 'MMMM') as month_name
    , date_format(full_date, 'EEEE') as weekday_name
    , weekofyear(full_date) as week_of_year
    , quarter(full_date) as quarter
    , case 
        when dayofweek(full_date) in (1,7) then true
        else false
      end as is_weekend

from date_spine
