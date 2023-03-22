{#
    This macro converts  numeric NaN columns to null when called
#}

{% macro convert_numeric_to_null(column_name) -%}

    case {{ column_name }} 
        when 'NaN'::numeric 
        then NULL
        else CAST({{column_name}} as numeric)
    end


{%- endmacro %}
