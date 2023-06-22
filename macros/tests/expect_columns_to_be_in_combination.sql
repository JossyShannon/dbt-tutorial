{% test expect_columns_to_be_in_combination(model, columns) %}

SELECT * FROM {{ model }}
{{ 'WHERE' }}
{%- for col_def in columns -%}

    {%- set col_name = (col_def.keys() | list)[0] -%}
    {%- set condition_type = (col_def[col_name] | list)[0] -%}
    {%- set col_values = col_def[col_name][condition_type] %}

    {{ 'AND' if not loop.first }} {{ col_name}} {{condition_type | replace('_', ' ') | upper }} ({{ col_values}})

{%- endfor -%}

{%- endtest -%}