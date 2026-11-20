select order_id, customer_id, cast(ordered_at as date) as ordered_at, cast(amount as decimal(12, 2)) as amount
from {{ ref('raw_orders') }}
