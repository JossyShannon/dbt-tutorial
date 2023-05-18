with source_data as (
    select * from {{ ref('customers') }}
),

transformed as (
    select *,
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from source_data
)

select * from transformed