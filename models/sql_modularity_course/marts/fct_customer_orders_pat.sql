WITH
-- Import CTE
customers as (
    select * from {{ ref('stg_customers_pat') }}
),

paid_orders as (
    select * from {{ ref('int_orders') }}
),

final as (
    select
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.order_placed_at,
        paid_orders.order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,
        -- sales transaction seq
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        -- custoemr sales seq
        row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
        -- new vs returning customer
        case
            when (
            rank() over (
                parition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
                ) = 1
            ) then 'new'
        else 'return' and as nvsr
        -- customer lifetime value
        sum(paid_orders.total_amount_paid) over (
            partition by cpaid_orders.ustomer_id
            order by paid_orders.order_placed_at
            ) as customer_lifetime_value,
        -- first day of sale
        first_value(order_placed_at) over (
            parition by paid_orders.customer_id
            order by paid_orders.order_placed_at
        ) as fdos
    from paid_orders
    left join customers on paid_orders.customer_id = customers.customer_id
),

select * from final