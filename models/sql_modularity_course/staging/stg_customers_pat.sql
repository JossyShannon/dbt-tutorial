with source_data as (
    select * from {{ source('jocelyn_shannon', 'customers') }}
),

transformed as (
    select id as customer_id,
        last_name as customer_last_name,
        first_name as customer_first_name,
        first_name || '' || last_name as full_name
    from source_data
)

select * from transformed