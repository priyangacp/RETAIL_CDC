-- test_no_future_timestamps ensures there are no records with timestamps set in the future

{% test no_future_timestamps(model, timestamp_column='LOAD_TIMESTAMP_UTC') %}
select * from {{model}} 
where {{timestamp_column}} > dateadd(hour,24,convert_timezone('UTC',current_timestamp())::timestamp_ntz)
{% endtest %}