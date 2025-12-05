select
    t.transaction_id as transaction_id,
    t.user_id as user_id,
    t.product_id as product_id,
    t.amount as amount,
    date_format(t.timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp,
    t.payment_method as payment_method,
    c.name as user_name,
    c.city as user_city,
    c.country as user_country
from transactions t
left join customers c
on
    t.user_id = c.user_id
where 
    t.user_id is not null
    and t.product_id is not null 
    and t.amount > 0 
    and t.timestamp <= current_timestamp