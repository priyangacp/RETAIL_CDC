{% macro replay_from_archive(queue_table_name, hook_type, database='RDL_TMS_SVT', schema='CDC') %}
-- ═══════════════════════════════════════════════════════════════════
--   CREATE TABLE FROM ARCHIVE OF <queue_table>
--   WHERE ts BETWEEN replay_watermark AND replay_end
--
-- This macro handles only SLP detach/attach around the MERGE.
-- The staging table creation is handled inside cdc_merge.sql
-- ═══════════════════════════════════════════════════════════════════

    {% set is_replay = var('replay_watermark', '1900-01-01 00:00:00') != '1900-01-01 00:00:00' %}

    {% if is_replay %}

        {% set queue_fqn = database ~ '.' ~ schema ~ '.' ~ queue_table_name %}

        {% if hook_type == 'pre' %}

            -- Detach SLP before MERGE so DML on queue is not blocked
            {{ log("REPLAY [pre]: Detaching SLP from " ~ queue_fqn, info=True) }}
            {{ detach_slp(queue_table_name, database=database, schema=schema) }}
            {{ log("REPLAY [pre]: SLP detached. Ready for MERGE.", info=True) }}

        {% elif hook_type == 'post' %}

            -- Re-attach SLP after MERGE completes
            {{ log("REPLAY [post]: Re-attaching SLP to " ~ queue_fqn, info=True) }}
            {{ attach_slp(queue_table_name, database=database, schema=schema) }}
            {{ log("REPLAY [post]: SLP re-attached.", info=True) }}

        {% else %}
            {{ exceptions.raise_compiler_error(
                "replay_from_archive: hook_type must be 'pre' or 'post', got '" ~ hook_type ~ "'"
            ) }}
        {% endif %}

    {% endif %}

{% endmacro %}