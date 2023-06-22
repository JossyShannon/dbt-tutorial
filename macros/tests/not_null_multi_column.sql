{%- test not_null_multi_column(model, columns=None, exclude_columns=None) -%}

    {%- if columns is none -%}
        {%- set columns_relation = get_columns_in_relation(model) -%}
        {%- set columns = [] -%}
        {%- for col in columns_relation -%}
            {%- if exclude_columns is not none -%}
                {%- if col.column | string | lower not in exclude_columns | lower -%}
                    {%- do columns.append(col.column) -%}
                {%- endif -%}
            {%- else -%} {%- do columns.append(col.column) -%}
            {%- endif %}
        {%- endfor -%}
    {%- endif -%}

    {% for col in columns %}
        {%- if loop.first -%}
            with
            {%- endif %} null_{{ col }} as (
                select count(*) as null_count, '{{ col }}' as column_name
                from {{ model }}
                where {{ col }} is null
            )
            {%- if not loop.last -%},{%- endif %}
    {% endfor %}

    {% for col in columns %}
        select *
        from null_{{ col }}
        where null_count != 0
        {% if not loop.last %}
            union all
        {%- endif -%}
    {%- endfor %}

{%- endtest -%}
