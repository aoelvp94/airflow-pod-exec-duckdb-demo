select customer_id, name, country from {{ ref('raw_customers') }}
