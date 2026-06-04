{% macro soft_delete_base(
    model_relation,
    exclude_deleted   = false,
    deleted_operation = 'D',
    archive_source    = 'ARCHCA22'
) %}

with base as (

    select *
    from {{ model_relation }}

),

with_soft_delete_flags as (

    select
        base.*,
        case
            when upper(trim(load_operation)) = '{{ deleted_operation }}'
             and upper(trim(data_source))   != '{{ archive_source }}'
            then true
            else false
        end as IS_DELETED,

        case
            when upper(trim(load_operation)) = '{{ deleted_operation }}'
             and upper(trim(data_source))   != '{{ archive_source }}'
            then load_timestamp_utc
            else null
        end as DELETED_AT

    from base

)

select *
from with_soft_delete_flags
{% if exclude_deleted %}
where IS_DELETED = false
{% endif %}

{% endmacro %}
