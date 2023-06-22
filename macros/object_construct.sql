{%- test object_construct(
    model, source_model, column_name, key_dict, keep_null=False
) -%}

    with
        expected as (
            select
                {%- if not keep_null %} object_construct(
                {%- elif keep_null %} object_construct_keep_null(
                {%- else %} object_construct(
                {%- endif -%}
                    {%- for col in key_dict -%}
                        '{{ col }}',
                        {%- if key_dict[col]["quote"] %} {{ key_dict[col]["value"] }}
                        {%- else -%} '{{ key_dict[col]["value"] }}'
                        {%- endif %}
                        {%- if not loop.last -%},{%- endif %}
                    {%- endfor -%}
                ) as {{ column_name }}
            from {{ source_model }}
        ),

        actual as (select object_test from {{ model }}),

        compare as (
            select *
            from expected
            except
            select *
            from actual
        )

    select *
    from compare

{%- endtest -%}
