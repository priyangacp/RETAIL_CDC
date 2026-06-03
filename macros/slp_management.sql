{% macro attach_slp(table_name, database='RDL_TMS_SVT', schema='CDC') %}
    {% set sql %}
        alter table {{ database }}.{{ schema }}.{{ table_name }}
        add storage lifecycle policy {{ database }}.{{ schema }}.cdc_queue_slp
        on (load_timestamp_utc);
    {% endset %}

    {{ log("Attaching SLP to " ~ database ~ "." ~ schema ~ "." ~ table_name, info=True) }}
    {% do run_query(sql) %}
    {{ log("SLP attachment complete.", info=True) }}
{% endmacro %}


{% macro detach_slp(table_name, database='RDL_TMS_SVT', schema='CDC') %}
    {% set sql %}
        alter table {{ database }}.{{ schema }}.{{ table_name }}
        drop storage lifecycle policy;
    {% endset %}

    {{ log("Detaching SLP from " ~ database ~ "." ~ schema ~ "." ~ table_name, info=True) }}
    {% do run_query(sql) %}
    {{ log("SLP detachment complete.", info=True) }}
{% endmacro %}


{% macro attach_slp_all_queues(database='RDL_TMS_SVT', schema='CDC') %}
    {% set tables = var('queue_tables', []) %}

    {% if tables | length == 0 %}
        {{ log("attach_slp_all_queues: `queue_tables` var is empty — nothing to attach.", info=True) }}
    {% else %}
        {% for tbl in tables %}
            {{ log("attach_slp_all_queues: processing " ~ tbl, info=True) }}
            {{ attach_slp(tbl, database=database, schema=schema) }}
        {% endfor %}
        {{ log("attach_slp_all_queues: done — " ~ tables | length ~ " table(s) processed.", info=True) }}
    {% endif %}

{% endmacro %}