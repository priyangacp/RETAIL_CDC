-- test_row_count_non_zero covers the model is not empty after run

{% test row_count_non_zero(model) %}
select 1 where (select count(*) from {{ model }})=0
{% endtest %}