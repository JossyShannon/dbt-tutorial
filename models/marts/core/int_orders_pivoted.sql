{%- set payment_methods = ['bank_transfer', 'credit_card', 'gift_card', 'coupon'] -%}

with payments as (
    select * from {{ ref('stg_payment') }}
),

pivoted as (
    select
        order_id,
        {% for method in payment_methods -%}
            {{ sum_case_when('payment_method', method) }}
            {%- if not loop.last -%}
                ,
            {%- endif %}
        {% endfor -%}
    from payments
    where status = 'success'
    group by 1
)

select * from pivoted