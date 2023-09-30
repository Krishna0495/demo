
select customer_id, max(order_date) as end_date,
min(order_date) as start_date

from
orders where customer_id in 
(
select customer_id, max(date_diffr) as unq_date_diff
from
(select 
customer_id,
order_date,
lag(order_date,1) over(partition by customer_id order by order_date asc) as lag_date,
date_diff(d,order_date,lag_date) as date_diffr
from
orders
) a
group by customer_id

) where unq_date_diff=1
group by customer_id



order_id, customer_id, order_date, amt

orders