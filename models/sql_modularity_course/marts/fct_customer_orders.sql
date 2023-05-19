with paid_orders as (
    
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.customer_first_name,
        c.customer_last_name
    from {{ ref('stg_orders') }} as orders
    left join {{ ref('stg_payments') }} p 
    on orders.order_id = p.order_id
    left join {{ ref('stg_customers') }} c 
    on orders.customer_id = c.customer_id 
    
),


customer_orders as (

    select 
        c.customer_id,
        min(orders.order_placed_at) as first_order_date,
        max(orders.order_placed_at) as most_recent_order_date,
        count(orders.order_id) as number_of_orders
    from {{ ref('stg_customers') }} c 
    left join {{ ref('stg_orders') }} as orders
    on orders.customer_id = c.customer_id 
    group by 1

),


final as (
    select
        p.*,
        row_number() 
            over (
            order by p.order_id
        ) as transaction_seq,
        row_number() 
            over (
            partition by customer_id order by p.order_id
        ) as customer_sales_seq,
        case 
        when c.first_order_date = p.order_placed_at 
        then 'new'
        else 'return' 
        end as nvsr,
        sum(total_amount_paid) over (
            partition by p.customer_id
            order by p.order_id
        ) as customer_lifetime_value,
        c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c using (customer_id)
    order by order_id

)


select * from final