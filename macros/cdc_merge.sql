{% macro cdc_merge(
    source_relation,
    target_relation,
    primary_key,
    live_schema='CA22',
    archive_schema='ARCHCA22'
) %}


{% set pk_upper  = primary_key | map('upper') | list %}
{% set is_replay = var('replay_watermark', '1900-01-01 00:00:00') != '1900-01-01 00:00:00' %}

-- ── staging table name built from replay_watermark date ──
{% if is_replay %}
    {% set replay_date   = var('replay_watermark')[:10] | replace('-','') %}
    {% set staging_table = target_relation.database ~ '.CA.'
                           ~ source_relation.identifier
                           ~ '_REPLAY_' ~ replay_date %}
{% endif %}

-- ── CREATE staging table from COOL archive ───────────────
-- Only runs during replay. Uses CREATE TABLE FROM ARCHIVE OF syntax
-- to restore rows from COOL tier into an active transient table
-- that the MERGE can query normally.
{% if is_replay %}
    {% set create_staging %}
        create or replace transient table {{ staging_table }} as
        select *
        from table(
            {{ source_relation }}!restore_archived_data(
                archive_timestamp_range => (
                    '{{ var("replay_watermark") }}'::timestamp_ntz,
                    '{{ var("replay_end") }}'::timestamp_ntz
                )
            )
        )
    {% endset %}

    {{ log("REPLAY: Creating transient staging table " ~ staging_table, info=True) }}
    {% do run_query(create_staging) %}
    {{ log("REPLAY: Staging table ready.", info=True) }}
{% endif %}

-- ── Determine which table to read from ── ──
-- Normal run : reads from the live queue (HOT tier, watermark filtered)
-- Replay run : reads from the staging table restored from COOL tier
{% if is_replay %}
    {% set read_relation = staging_table %}
{% else %}
    {% set read_relation = source_relation %}
{% endif %}

{% set cdc_enriched_query %}

    -- ── Step 1: read rows from correct source ── ──
    -- WHERE LOAD_TIMESTAMP_UTC >= MAX(base) - 1hr
    -- WHERE LOAD_TIMESTAMP_UTC >= LEAST(MAX(base), watermark)
    with cdc_data as (
        select *
        from {{ read_relation }}
        {% if is_incremental() and not is_replay %}
            where load_timestamp_utc >= (
                select dateadd(hour, -1,
                    coalesce(
                        max(load_timestamp_utc),
                        '1900-01-01'::timestamp_ntz
                    )
                )
                from {{ target_relation }}
            )
        {% elif is_replay %}
            -- watermark lowered via LEAST() so archived
            -- rows in the replay window pass the incremental filter
            where load_timestamp_utc >= (
                select least(
                    coalesce(max(load_timestamp_utc), '1900-01-01'::timestamp_ntz),
                    '{{ var("replay_watermark") }}'::timestamp_ntz
                )
                from {{ target_relation }}
            )
        {% endif %}
    ),

    -- ── Step 2: archive detection window functions (TDD section 5.4) ──
    -- Must run on RAW data before dedup so both sides of a split-batch
    -- archive pair (CA22/D + ARCHCA22/I) are always visible.
    archive_logic as (
        select
            *,
            max(
                case
                    when upper(trim(data_source))    = '{{ live_schema }}'
                     and upper(trim(load_operation)) = 'D'
                    then 1 else 0
                end
            ) over (partition by {{ primary_key | join(', ') }}) as has_active_delete,

            max(
                case
                    when upper(trim(data_source)) = '{{ archive_schema }}'
                    then 1 else 0
                end
            ) over (partition by {{ primary_key | join(', ') }}) as has_archive_record,

            -- Archive priority sort key (TDD section 5.3 / GAP-03)
            -- ARCHCA22 rows = 0 so they always win dedup on SCN tie
            case
                when upper(trim(data_source)) = '{{ archive_schema }}'
                then 0 else 1
            end as _archive_priority
        from cdc_data
    ),

    -- ── Step 3: deduplicate — one row per PK (TDD section 5.3) ───────
    deduplicated as (
        select *
        from archive_logic
        qualify row_number() over (
            partition by {{ primary_key | join(', ') }}
            order by
                _archive_priority asc,
                load_cdc_scn desc,
                load_cdc_sequence_internal desc
        ) = 1
    ),

    -- ── Step 4: resolve final operation and source (TDD section 5.2) ──
    final_source as (
        select
            * exclude (_archive_priority),
            case
                when has_active_delete = 1 and has_archive_record = 1
                then 'U'
                else load_operation
            end as final_load_operation,
            case
                when has_active_delete = 1 and has_archive_record = 1
                then '{{ archive_schema }}'
                else data_source
            end as final_data_source
        from deduplicated
    ),

    -- ── Step 5: SCN guard — replay only ── ──
    -- LEFT JOIN on base table to filter out rows where base already
    -- holds a newer version. New rows (tgt.PK IS NULL) always pass.
    -- Normal hourly runs skip this entirely — zero overhead.
    {% if is_replay %}
    scn_guarded as (
        select src.*
        from final_source src
        left join {{ target_relation }} tgt
            on {% for col in primary_key %}
               src.{{ col }} = tgt.{{ col }}
               {% if not loop.last %} and {% endif %}
               {% endfor %}
        where tgt.{{ primary_key[0] }} is null
           or src.load_cdc_scn > tgt.load_cdc_scn
           or (
                src.load_cdc_scn = tgt.load_cdc_scn
                and src.load_cdc_sequence_internal > tgt.load_cdc_sequence_internal
              )
    )
    {% else %}
    scn_guarded as (
        select * from final_source
    )
    {% endif %}

    select * from scn_guarded

{% endset %}

-- ── MERGE (TDD section 7.4 / 7.5) ── ──
merge into {{ target_relation }} as tgt
using ( {{ cdc_enriched_query }} ) as src
on
    {% for col in primary_key %}
    src.{{ col }} = tgt.{{ col }}{% if not loop.last %} and {% endif %}
    {% endfor %}

when matched and src.final_load_operation in ('U', 'D')
then update set
    {% set ns = namespace(first=true) %}
    {% for col in adapter.get_columns_in_relation(source_relation) %}
        {% if col.name | upper not in pk_upper
              and col.name | upper not in [
                  'FINAL_LOAD_OPERATION', 'FINAL_DATA_SOURCE',
                  'HAS_ACTIVE_DELETE', 'HAS_ARCHIVE_RECORD',
                  'LOAD_OPERATION', 'DATA_SOURCE'
              ] %}
            {% if not ns.first %}, {% endif %}
            tgt.{{ col.name }} = src.{{ col.name }}
            {% set ns.first = false %}
        {% endif %}
    {% endfor %}
    , tgt.load_operation = src.final_load_operation
    , tgt.data_source    = src.final_data_source

when not matched and src.final_load_operation in ('I', 'U')
then insert (
    {% for col in adapter.get_columns_in_relation(source_relation) %}
        {{ col.name }}{% if not loop.last %}, {% endif %}
    {% endfor %}
) values (
    {% for col in adapter.get_columns_in_relation(source_relation) %}
        src.{{ col.name }}{% if not loop.last %}, {% endif %}
    {% endfor %}
)

-- ── Post-replay cleanup: drop transient staging table ── ──
-- staging table is transient and dropped after MERGE
{% if is_replay %}
    {% set drop_staging %}
        drop table if exists {{ staging_table }}
    {% endset %}
    {% do run_query(drop_staging) %}
    {{ log("REPLAY: Staging table " ~ staging_table ~ " dropped.", info=True) }}
{% endif %}

{% endmacro %}