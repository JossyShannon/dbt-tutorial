{%- macro sum_case_when(column_name, identifier, string=flase, alias=true) -%}
    sum(case when {{ column_name }} = 
    {%- if string == false -%}
        {{ identifier }} 
    {%- else -%}
        '{{identifier}}' 
    {%- endif -%}
     then amount else 0 end) 
    {%- if alias == true -%}
        as {{ identifier }}_amount
    {%- elif alias == flase -%}
    {%- elif alias == None -%}
        as amount
    {%- endif -%}
{%- endmacro -%}