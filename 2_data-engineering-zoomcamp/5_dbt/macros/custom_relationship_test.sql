{# macros/custom_relationship_test.sql #}

{% macro test_custom_relationship_test(model, column_name, ref_model, ref_column) %}

with parent as (

    select {{ column_name }} as id
    from {{ model }}

),

child as (

    select "{{ ref_column }}" as id
    from {{ ref_model }}

)

select count(*)
from parent
where id not in (select id from child)

{% endmacro %}