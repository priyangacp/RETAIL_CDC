-- test_archival_operation_valid verifies the record marked archival has correct load operation

{% test archival_operation_valid(model,archival_data_source='ARCHCA22',expected_operation='U') %}
select * from {{model}} 
where upper(trim(DATA_SOURCE))='{{archival_data_source}}' and upper(trim(LOAD_OPERATION)) != '{{expected_operation}}'
{% endtest %}