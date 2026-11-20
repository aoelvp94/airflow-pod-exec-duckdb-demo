select
    c.customer_id,
    c.name,
    c.country,
    count(o.order_id) as orders,
    coalesce(sum(o.amount), 0) as revenue
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o using (customer_id)
group by all
