-- test_unique_composite_key covers the primary key for model is unique

{% test unique_composite_key(model, key_columns) %}

select
    {{ key_columns | join(', ') }},
    count(*) as row_cnt
from {{ model }}
group by {{ key_columns | join(', ') }}
having count(*) > 1

{% endtest %}