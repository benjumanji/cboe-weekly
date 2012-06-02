with 
open_close_gap as
(
  select
      business_date
    , symbol
    , open
    , close
    , close - open as o_to_c
    , open - lag(close) over (partition by symbol order by business_date asc) as c_to_o
    , close - lag(close) over (partition by symbol order by business_date asc) as c_to_c
  from 
      daily_price
),
open_close_stats as
(
  select 
      business_date
    , symbol
    , open
    , close
    , o_to_c
    , c_to_o
    , c_to_c
    , avg(o_to_c) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as mean_o_to_c
    , avg(c_to_o) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as mean_c_to_o
    , avg(c_to_c) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as mean_c_to_c
    , stddev_pop(o_to_c) over 
      (
        partition by symbol 
        order by business_date 
        asc rows 20 preceding
      ) as std_dev_o_to_c
    , stddev_pop(c_to_o) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as std_dev_c_to_o
    , stddev_pop(c_to_c) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as std_dev_c_to_c
  from open_close_gap
),
open_close_stats_ex as
(
  select
      business_date
    , symbol
    , open
    , close
    , o_to_c
    , c_to_o
    , c_to_c
    , mean_o_to_c
    , mean_c_to_o
    , mean_c_to_c
    , std_dev_o_to_c
    , std_dev_c_to_o
    , std_dev_c_to_c
    , count(case when (@ o_to_c) > 3 * std_dev_o_to_c then 1 else null end) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as o_to_c_3x_price_spike
    , count(case when (@ c_to_o) > 3 * std_dev_c_to_o then 1 else null end) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as c_to_o_3x_price_spike
    , count(case when (@ c_to_c) > 3 * std_dev_c_to_c then 1 else null end) over 
      (
        partition by symbol 
        order by business_date asc 
        rows 20 preceding
      ) as c_to_c_3x_price_spike
  from
    open_close_stats
)
select * from open_close_stats_ex;
