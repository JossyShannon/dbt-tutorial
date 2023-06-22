{%- test accepted_values_multi_column(model, column_names_and_values, quote=True) %}

with 
    {% for col in column_names_and_values -%}
        all_values_{{ col }} as (

            select
                {{ col }} as value_field,
                count(*) as n_records

            from {{ model }}
            group by {{ col }}

        ), 

        value_{{ col }} as (
            select *
            from all_values_{{ col }}
            where value_field not in (
                {% for value in column_names_and_values[col] -%}
                    {% if quote -%}
                    '{{ value }}'
                    {%- else -%}
                    {{ value }}
                    {%- endif -%}
                    {%- if not loop.last -%},{%- endif %}
                {%- endfor %}
            ) 
        ){%- if not loop.last %}, {%- endif %}
    {%- endfor %}

    {% for col in column_names_and_values %}
    select *, '{{ col }}' as column_name from value_{{ col }}
    {% if not loop.last %} UNION ALL {%- endif %}
    {% endfor %}

{%- endtest -%}
