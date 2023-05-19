with source_data as(
    select * from {{ source('jaffle_shop', 'payments') }}
),

transformed as (
    select
        id as payment_id,
        orderid as order_id,
        created as payment_created_at,
        status as payment_status,
        round(amount/100.0, 1) as payment_amount
    from source_data
)

select * from transformed